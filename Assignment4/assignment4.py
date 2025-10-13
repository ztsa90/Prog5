#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Assignment 4 — Parse NCBI RefSeq (Archaea) GenBank (.gbff) file
and load the data into a MariaDB database.

High-level flow:
  1) Connect to MariaDB using credentials in ~/.my.cnf (never hardcode creds).
  2) Ensure tables exist (CREATE IF NOT EXISTS), then clear them (TRUNCATE).
  3) Parse a GenBank (.gbff) file using Biopython (SeqIO.parse).
  4) Extract and insert:
       - Species: scientific name + NCBI taxonomy ID
       - Genomes: accession + sizes + counts + FK to species
       - Proteins (CDS): protein_id + product + locus_tag + EC + GO + location + FK to genome
  5) Insert proteins in **batches of 500** to balance speed and memory.

Design choices explained in comments throughout.
"""

# ----------------------------------------------------------
# Imports
# ----------------------------------------------------------
# SQLAlchemy: create_engine creates a DB connection pool; text lets us write raw SQL safely.
from sqlalchemy import create_engine, text

# Biopython SeqIO: parses GenBank files into SeqRecord objects with .features and .annotations.
from Bio import SeqIO

# time is used only for simple timing/printing of how long each step takes (progress).
import time


# ----------------------------------------------------------
# Global settings / constants
# ----------------------------------------------------------
GBFF_FILE = (
    "/data/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/refseq/release/"
    "archaea/archaea.1.genomic.gbff"
)
# BATCH controls how many Protein rows we send to the DB in one INSERT.
# Why batching? Sending 1-by-1 is slow; sending too many can hit memory/packet limits.
# 500 is a reasonable compromise for stability + speed on shared systems.
BATCH = 500

# Create the SQLAlchemy engine:
# - The DSN points to your database/schema (Ztaherihanjani) on mariadb.bin.bioinf.nl
# - Credentials (user/password) are read from ~/.my.cnf so *no secrets* in code.
# - pool_pre_ping=True verifies connections before using them to avoid stale-connection errors.
engine = create_engine(
    "mysql+mysqldb://@mariadb.bin.bioinf.nl:3306/Ztaherihanjani",
    connect_args={"read_default_file": "/homes/ztaherihanjani/.my.cnf"},
    pool_pre_ping=True,
)


# ----------------------------------------------------------
# Database setup helpers
# ----------------------------------------------------------
def create_tables_if_needed():
    """
    Create tables if they do not exist.
    We do NOT drop tables here: that avoids lock issues and is safer on a shared DB.

    We keep schemas simple and normalized:
      - Species(taxdb_id PK, name)
      - Genome(accession PK, species_id FK -> Species)
      - Protein(id AUTO PK, genome_accession FK -> Genome, plus protein fields)

    Note: We do *not* use protein_id as a PK because the same WP_ protein
    can appear in multiple genomes; using AUTO_INCREMENT 'id' avoids duplicates.
    """
    with engine.begin() as conn:  # begin() → transaction that auto-commits or rolls back
        # Relax timeouts for DDL to avoid spurious failures on shared servers.
        conn.execute(text("SET SESSION max_statement_time=0"))
        conn.execute(text("SET SESSION lock_wait_timeout=60"))
        conn.execute(text("SET SESSION innodb_lock_wait_timeout=60"))

        # Species: taxdb_id is the stable NCBI taxonomy ID (string), primary key.
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS Species (
                taxdb_id VARCHAR(50) PRIMARY KEY,  -- e.g., "2508724"
                name     VARCHAR(255)              -- e.g., "Halorubrum amylolyticum"
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))

        # Genome: accession is the stable (with version) ID of that genome record; FK links to species.
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS Genome (
                accession     VARCHAR(50) PRIMARY KEY,  -- e.g., "NZ_SDJP01000015.1"
                genome_size   INT,                      -- bp length of the record (len(rec.seq))
                num_genes     INT,                      -- count of "gene" features in the record
                num_proteins  INT,                      -- count of "CDS" features in the record
                species_id    VARCHAR(50),              -- NCBI taxonomy ID (FK to Species.taxdb_id)
                FOREIGN KEY (species_id) REFERENCES Species(taxdb_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))

        # Protein: we use an AUTO_INCREMENT surrogate PK 'id'.
        # 'protein_id' (e.g., WP_/NP_/YP_) is NOT unique globally (can appear in multiple genomes),
        # so we index it but do not enforce uniqueness on it.
        # 'genome_accession' FK ties protein rows to the specific genome record they came from.
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS Protein (
                id               BIGINT AUTO_INCREMENT PRIMARY KEY,  -- simple row identifier
                protein_id       VARCHAR(50),                        -- e.g., "WP_123456789.1"
                genome_accession VARCHAR(50) NOT NULL,               -- FK -> Genome(accession)
                product_name     TEXT,                               -- descriptive name of protein
                locus_tag        VARCHAR(50),                        -- stable gene/locus identifier
                gene_ref         VARCHAR(50),                        -- gene symbol if available
                ec_number        VARCHAR(50),                        -- "EC x.x.x.x" if annotated
                go_annotations   TEXT,                               -- "GO:..." terms; semicolon-joined
                location         VARCHAR(100),                       -- feature coordinates as string
                FOREIGN KEY (genome_accession) REFERENCES Genome(accession)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))


def truncate_tables():
    """
    Remove all data from Protein, Genome, Species (in child→parent order).
    We TRUNCATE instead of DROP to:
      - keep the schema (no re-creation needed),
      - reset auto-increment counters,
      - avoid long metadata locks from DROP/CREATE loops.

    FOREIGN_KEY_CHECKS=0 allows truncating parent while children exist, safely in sequence.
    """
    with engine.begin() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        conn.execute(text("TRUNCATE TABLE Protein"))
        conn.execute(text("TRUNCATE TABLE Genome"))
        conn.execute(text("TRUNCATE TABLE Species"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))


# ----------------------------------------------------------
# GenBank parsing helpers
# ----------------------------------------------------------
def get_one(feat, key):
    """
    Return the first value of a feature qualifier (e.g., "product", "locus_tag").
    Why first? In GenBank, qualifiers are lists (can have multiple values).
    For most fields we store only one (the primary/first) for simplicity.

    Example:
      vals = ["helix-turn-helix protein"] -> returns that string
      vals = [] -> returns None
    """
    vals = feat.qualifiers.get(key, [])
    return vals[0] if vals else None


def get_ec(feat):
    """
    Collect all EC (Enzyme Commission) numbers on a feature.
    A CDS may have 0, 1, or multiple EC numbers. We join multiple with ';' for compact storage.
    If none exist, return None to keep DB clean.

    Example output: "1.1.1.1;2.7.7.7"
    """
    ecs = feat.qualifiers.get("EC_number", [])
    return ";".join(ecs) if ecs else None


def get_go(feat):
    """
    Collect all GO (Gene Ontology) accessions from db_xref qualifiers.
    db_xref is a list of cross-references ("GO:0005524", "InterPro:...", etc.).
    We select only those starting with "GO:", deduplicate, sort, and ';'-join.

    Example output: "GO:0005524;GO:0016887"
    """
    out = []
    for x in feat.qualifiers.get("db_xref", []):
        if isinstance(x, str) and x.startswith("GO:"):
            out.append(x)
    return ";".join(sorted(set(out))) if out else None


def extract_species_from_record(rec):
    """
    From one SeqRecord (a GenBank record), extract:
      - species_name: rec.annotations["organism"], e.g., "Halorubrum amylolyticum"
      - taxon_id: the 'taxon:NNNN' value found under the 'source' feature's db_xref

    We loop features to find the 'source' feature because that's where NCBI places taxon info.
    """
    species_name = rec.annotations.get("organism", None)
    taxon_id = None

    for feat in rec.features:
        if feat.type == "source":
            # db_xref is a list like ["taxon:2508724", "BioProject:PRJNA..."]
            for x in feat.qualifiers.get("db_xref", []):
                if isinstance(x, str) and x.startswith("taxon:"):
                    taxon_id = x.split(":", 1)[1]  # take the numeric part after "taxon:"
                    break
        if taxon_id:
            break

    return species_name, taxon_id


def extract_genome_from_record(rec):
    """
    Build a dictionary representing one row for the Genome table from a SeqRecord.

    Fields:
      - accession: rec.id (already includes the version, e.g., ".1")
      - genome_size: len(rec.seq) (bp)
      - num_genes: count of features where f.type == "gene"
      - num_proteins: count of features where f.type == "CDS"
      - species_id: taxon_id extracted via extract_species_from_record()

    This function keeps parsing logic in one place, making main code cleaner.
    """
    accver = rec.id  # the GenBank record ID; usually the accession + version (e.g., NZ_xxx.1)

    # Some rare records might not have sequence loaded; guard with None check.
    genome_size = len(rec.seq) if rec.seq is not None else None

    # Gene/protein counts: simple feature-type counts across this record.
    num_genes = sum(1 for f in rec.features if f.type == "gene")
    num_proteins = sum(1 for f in rec.features if f.type == "CDS")

    # species_id is the NCBI taxonomy ID; used as FK to Species table.
    _, taxon_id = extract_species_from_record(rec)

    return {
        "accession": accver,
        "genome_size": genome_size,
        "num_genes": num_genes,
        "num_proteins": num_proteins,
        "species_id": taxon_id,
    }


# ----------------------------------------------------------
# Loaders: each table loaded in its own pass (clear output + timing)
# ----------------------------------------------------------
def load_species():
    """
    Parse all records once and collect unique (taxdb_id, name) pairs in a set
    to avoid inserting duplicates. Then do INSERT IGNORE for safety:
      - If the row exists already, MySQL skips it without error.
      - This keeps the script idempotent (safe to run many times).
    """
    print(f"📖 Step 1: Parsing species from {GBFF_FILE}")
    start = time.time()

    species_set = set()  # set automatically de-duplicates by (taxid, name)
    for record in SeqIO.parse(GBFF_FILE, "genbank"):
        name, taxid = extract_species_from_record(record)
        if taxid and name:
            species_set.add((taxid, name))

    # Insert every unique species into DB
    with engine.begin() as conn:
        for taxid, name in species_set:
            conn.execute(
                text("INSERT IGNORE INTO Species (taxdb_id, name) VALUES (:t, :n)"),
                {"t": taxid, "n": name},
            )

    print(f"✅ Step 1 complete — {len(species_set)} species ({time.time() - start:.2f}s)\n")
    return len(species_set)


def load_genomes():
    """
    Parse all records again and build a dict: accession -> row dict.
    Why a dict? If the same accession appears multiple times, the dict
    keeps only one (last wins), avoiding duplicate inserts.

    We then INSERT IGNORE each genome row to avoid errors if a row already exists.
    """
    print("📖 Step 2: Parsing genomes...")
    start = time.time()

    genome_rows = {}  # maps accession to row dict
    for rec in SeqIO.parse(GBFF_FILE, "genbank"):
        row = extract_genome_from_record(rec)
        if row["accession"]:
            genome_rows[row["accession"]] = row  # overwrite duplicates if any

    with engine.begin() as conn:
        for row in genome_rows.values():
            conn.execute(text("""
                INSERT IGNORE INTO Genome
                  (accession, genome_size, num_genes, num_proteins, species_id)
                VALUES
                  (:accession, :genome_size, :num_genes, :num_proteins, :species_id)
            """), row)

    print(f"✅ Step 2 complete — {len(genome_rows)} genomes ({time.time() - start:.2f}s)\n")
    return len(genome_rows)


def load_proteins():
    """
    Parse all records again, but this time focus on CDS features (protein-coding regions).

    For each CDS feature:
      - protein_id: a stable protein identifier (e.g., WP_... .1). Some CDS lack this (skip those).
      - product_name: a human-readable protein description (qualifier "product")
      - locus_tag: stable gene/locus identifier on this genome (qualifier "locus_tag")
      - gene_ref: gene symbol if present (qualifier "gene")
      - ec_number: enzyme classification numbers; multiple joined by ';'
      - go_annotations: collected GO terms from db_xref; multiple joined by ';'
      - location: string version of feature coordinates (Biopython location → str)

    We batch INSERT to the DB:
      - 'buffer' collects up to BATCH rows (here 500).
      - When buffer is full, we execute a single parameterized INSERT with all rows.
      - This is much faster than inserting one row at a time and avoids memory spikes.

    Returns the total number of protein rows inserted.
    """
    print("📖 Step 3: Parsing and inserting proteins...")
    start = time.time()

    inserted = 0          # running count of inserted rows
    buffer = []           # temporary storage for one batch

    with engine.begin() as conn:
        for rec in SeqIO.parse(GBFF_FILE, "genbank"):
            genome_acc = rec.id  # FK target to Genome(accession)
            for feat in rec.features:
                # We only care about coding sequences (CDS), which represent proteins.
                if feat.type != "CDS":
                    continue

                # Not all CDS have a 'protein_id' (e.g., pseudogenes). We skip those to avoid NULLs.
                pid = get_one(feat, "protein_id")
                if not pid:
                    continue

                # Build a dict mapping column names to values for this protein row.
                # Note: get_one returns only the first value of a qualifier list; that's enough here.
                buffer.append({
                    "protein_id": pid,
                    "genome_accession": genome_acc,
                    "product_name": get_one(feat, "product"),
                    "locus_tag": get_one(feat, "locus_tag"),
                    "gene_ref": get_one(feat, "gene"),
                    "ec_number": get_ec(feat),
                    "go_annotations": get_go(feat),
                    "location": str(feat.location),  # keep the Biopython location string as-is
                })

                # If we've reached the batch size, push to DB and clear the buffer.
                if len(buffer) >= BATCH:
                    conn.execute(text("""
                        INSERT INTO Protein
                          (protein_id, genome_accession, product_name, locus_tag, gene_ref,
                           ec_number, go_annotations, location)
                        VALUES
                          (:protein_id, :genome_accession, :product_name, :locus_tag, :gene_ref,
                           :ec_number, :go_annotations, :location)
                    """), buffer)
                    inserted += len(buffer)
                    print(f"   → inserted {inserted:,} rows...")
                    buffer.clear()

        # After the loop, there may be leftover rows < BATCH; flush them once more.
        if buffer:
            conn.execute(text("""
                INSERT INTO Protein
                  (protein_id, genome_accession, product_name, locus_tag, gene_ref,
                   ec_number, go_annotations, location)
                VALUES
                  (:protein_id, :genome_accession, :product_name, :locus_tag, :gene_ref,
                   :ec_number, :go_annotations, :location)
            """), buffer)
            inserted += len(buffer)
            buffer.clear()

    print(f"✅ Step 3 complete — {inserted:,} proteins ({time.time() - start:.2f}s)\n")
    return inserted


# ----------------------------------------------------------
# Main entry point
# ----------------------------------------------------------
def main():
    """
    Orchestrates the full ETL:
      - Connect (engine is already created globally)
      - Create tables (idempotent)
      - Clear old rows (TRUNCATE)
      - Load species, genomes, and proteins (with timing + progress prints)
    """
    total_start = time.time()

    print("🔌 Connecting to database...")
    print("✅ Connected!\n")

    # Make sure schema exists, then start fresh with empty tables.
    print("🗄️  Preparing tables (create + truncate)...")
    create_tables_if_needed()
    truncate_tables()
    print("✅ Tables ready (empty and clean)\n")

    # Load each layer in order: Species → Genome → Protein (FK dependencies are respected).
    n_species = load_species()
    n_genomes = load_genomes()
    n_proteins = load_proteins()

    # Summary at the end for quick reporting.
    total = time.time() - total_start
    print("=" * 60)
    print("🎉 All done! Summary:")
    print(f"   • Species inserted:  {n_species}")
    print(f"   • Genomes inserted:  {n_genomes}")
    print(f"   • Proteins inserted: {n_proteins}")
    print(f"   • Total time:        {total:.2f} seconds (~{total/60:.1f} min)")
    print("=" * 60)


# Only run main() when the script is executed directly (not when imported as a module).
if __name__ == "__main__":
    main()
