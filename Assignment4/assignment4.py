#!/usr/bin/env python3
"""
assignment4.py — Programming 5 (SQL)

Read a GenBank (.gb/.gbff[.gz]) Archaea file and store data in MariaDB:
- species (per record)   → name, accession+version, genome size, taxid, counters
- genes (per record)     → locus_tag, gene name, location (start/end/strand/text)
- proteins (CDS)         → protein_id, product, EC, GO, location, link to gene

Connection settings are read from ~/.my.cnf (the [client] section).
We purposely use raw SQL (CREATE/INSERT/UPDATE/SELECT) — no ORM.
"""

from __future__ import annotations

import gzip                    # to open .gz files as text
import os                      # to expand '~' and check paths
import sys                     # to read argv and exit codes
from configparser import ConfigParser  # to read ~/.my.cnf

from Bio import SeqIO          # Biopython sequence I/O
from sqlalchemy import create_engine, text  # SQLAlchemy (DB engine + raw SQL)

# ============================== Schema (DDL) ==================================
# Keep DDL in constants so they are easy to see and reuse.

DDL_SPECIES = (
    "CREATE TABLE IF NOT EXISTS species ("
    "  id INT AUTO_INCREMENT PRIMARY KEY,"
    "  name VARCHAR(255),"
    "  accession VARCHAR(64),"
    "  genome_size_bp INT,"
    "  n_genes INT,"
    "  n_proteins INT,"
    "  taxid INT"
    ") ENGINE=InnoDB;"
)

DDL_GENES = (
    "CREATE TABLE IF NOT EXISTS genes ("
    "  id INT AUTO_INCREMENT PRIMARY KEY,"
    "  species_id INT,"
    "  locus_tag VARCHAR(128),"
    "  gene_name VARCHAR(255),"
    "  location TEXT,"
    "  strand CHAR(1),"
    "  start_bp INT,"
    "  end_bp INT,"
    "  FOREIGN KEY (species_id) REFERENCES species(id)"
    ") ENGINE=InnoDB;"
)

DDL_PROTEINS = (
    "CREATE TABLE IF NOT EXISTS proteins ("
    "  id INT AUTO_INCREMENT PRIMARY KEY,"
    "  species_id INT,"
    "  gene_id INT,"
    "  protein_id VARCHAR(64),"
    "  product_name TEXT,"
    "  ec_number VARCHAR(64),"
    "  go_terms TEXT,"
    "  location TEXT,"
    "  FOREIGN KEY (species_id) REFERENCES species(id),"
    "  FOREIGN KEY (gene_id) REFERENCES genes(id)"
    ") ENGINE=InnoDB;"
)

# ============================== DB utilities ==================================


def db_url_from_mycnf(path: str = "~/.my.cnf") -> str:
    """
    Build a SQLAlchemy connection URL using ~/.my.cnf ([client] section).

    Expected keys under [client]:
      user=..., password=..., host=..., database=...

    Returns a string like:
      "mysql+pymysql://USER:PASSWORD@HOST/DATABASE"
    """
    cfg = ConfigParser()  # create a parser object
    cfg.read(os.path.expanduser(path))  # read the file (expand '~' to full path)

    if "client" not in cfg:
        # Fail fast if the expected section is not present
        raise RuntimeError("Missing [client] section in ~/.my.cnf")

    section = cfg["client"]              # access the [client] section
    user = section.get("user")           # MySQL user name
    password = section.get("password")   # MySQL password
    host = section.get("host", "mariadb.bin.bioinf.nl")  # default BIN server
    database = section.get("database")   # your personal database name

    if not (user and password and database):
        # All three are required to connect
        raise RuntimeError(
            "user/password/database missing in ~/.my.cnf [client]"
        )

    # Assemble a SQLAlchemy URL that uses the PyMySQL driver
    return f"mysql+pymysql://{user}:{password}@{host}/{database}"


def engine_from_cnf():
    """
    Create a SQLAlchemy engine from ~/.my.cnf.

    pool_recycle avoids 'server has gone away' on very long runs.
    """
    url = db_url_from_mycnf()                # read creds and build URL
    return create_engine(url, pool_recycle=3600)  # create the engine

# ================================ Helpers =====================================


def open_any(path: str):
    """
    Open a file for reading text:
      - if the name ends with '.gz' → open with gzip in text mode
      - else → open as regular UTF-8 text
    """
    if path.endswith(".gz"):
        # gzip.open(..., "rt") returns a text stream from a gzipped file
        return gzip.open(path, "rt")
    # open as normal text; errors='replace' avoids crashing on odd bytes
    return open(path, "rt", encoding="utf-8", errors="replace")


def parse_location(loc):
    """
    Convert a Biopython location into simple values.

    Returns:
      start (1-based int or None),
      end   (int or None),
      strand ('+' | '-' | None),
      loc_text (original location string)
    """
    try:
        # Biopython locations are 0-based; show 1-based start for humans
        start_1based = int(loc.start) + 1
        end = int(loc.end)
        # strand is +1, -1, or None → render as '+' / '-' / None
        strand = "+" if (loc.strand is None or loc.strand >= 0) else "-"
        return start_1based, end, strand, str(loc)
    except Exception:
        # If anything goes wrong, keep something informative
        return None, None, None, str(loc)


def extract_taxid(record):
    """
    Find NCBI taxonomy id on the 'source' feature, if present.

    Looks for a db_xref like 'taxon:XXXXX' and returns that number (int).
    """
    for feat in record.features:
        if feat.type != "source":
            # Only the 'source' feature carries the taxon cross-refs
            continue
        for xref in feat.qualifiers.get("db_xref", []):
            if xref.startswith("taxon:"):
                # Split at ':' and take the numeric part
                try:
                    return int(xref.split(":", 1)[1])
                except Exception:
                    return None
    return None

# ================================== Main ======================================


def main() -> int:
    """
    Program entry point.

    Usage:
      python3 assignment4.py /path/to/archaea.X.genomic.gbff[.gz]
    """
    # ---- 1) Validate CLI input ------------------------------------------------
    if len(sys.argv) < 2:
        # We require one argument: the GBFF/GB file path
        print("Usage: assignment4.py /path/to/archaea.X.genomic.gbff[.gz]")
        return 1

    gbff_path = sys.argv[1]  # first positional argument is the input file path

    if not os.path.exists(gbff_path):
        # Fail early if the path is wrong
        print(f"Input not found: {gbff_path}", file=sys.stderr)
        return 2

    # ---- 2) Create DB engine and ensure tables exist --------------------------
    engine = engine_from_cnf()  # build an engine using ~/.my.cnf

    # Use a transaction (BEGIN ... COMMIT). If something fails, it rolls back.
    with engine.begin() as conn:
        # Disable server-side statement timeout for long inserts (BIN setting)
        conn.execute(text("SET SESSION max_statement_time=0"))
        # Create the three tables if they do not exist yet
        conn.execute(text(DDL_SPECIES))
        conn.execute(text(DDL_GENES))
        conn.execute(text(DDL_PROTEINS))

    # ---- 3) Parse GBFF and insert data ---------------------------------------
    # open_any handles both plain text and .gz transparently
    with open_any(gbff_path) as handle:
        # SeqIO.parse yields SeqRecord objects for each record in the file
        for record in SeqIO.parse(handle, "genbank"):
            # ========== species row (one per record) ==========================
            species_name = record.annotations.get("organism")   # species name
            accession = record.id                                # includes version
            genome_size = len(record.seq)                        # length in bp
            taxid = extract_taxid(record)                        # NCBI taxid or None

            # Start a transaction for this record: insert species, genes, proteins
            with engine.begin() as conn:
                # Again disable statement timeout in this session
                conn.execute(text("SET SESSION max_statement_time=0"))

                # Insert species and capture the auto-incremented id
                insert_species_sql = text(
                    "INSERT INTO species "
                    "(name, accession, genome_size_bp, n_genes, n_proteins, taxid) "
                    "VALUES (:n, :a, :s, 0, 0, :t)"
                )
                res = conn.execute(
                    insert_species_sql,
                    {"n": species_name, "a": accession, "s": genome_size, "t": taxid},
                )
                species_id = res.lastrowid  # integer id of the new species row

                # ========== genes (collect id per locus_tag) ==================
                locus_to_gid = {}  # map locus_tag → genes.id for linking CDS later
                genes = [f for f in record.features if f.type == "gene"]

                for gene_feat in genes:
                    locus_tag = gene_feat.qualifiers.get("locus_tag", [None])[0]
                    gene_name = gene_feat.qualifiers.get("gene", [None])[0]
                    start, end, strand, loc_text = parse_location(gene_feat.location)

                    insert_gene_sql = text(
                        "INSERT INTO genes "
                        "(species_id, locus_tag, gene_name, location, strand, start_bp, end_bp) "
                        "VALUES (:sid, :lt, :gn, :loc, :st, :s, :e)"
                    )
                    r = conn.execute(
                        insert_gene_sql,
                        {
                            "sid": species_id,
                            "lt": locus_tag,
                            "gn": gene_name,
                            "loc": loc_text,
                            "st": strand,
                            "s": start,
                            "e": end,
                        },
                    )
                    locus_to_gid[locus_tag] = r.lastrowid  # remember gene id

                # ========== proteins (CDS) ===================================
                protein_count = 0
                cds_features = (f for f in record.features if f.type == "CDS")

                for cds in cds_features:
                    # protein_id may be absent for some entries → default None
                    protein_id = cds.qualifiers.get("protein_id", [None])[0]
                    product = cds.qualifiers.get("product", [None])[0]
                    # locus_tag used to link CDS → its parent gene (if present)
                    locus_tag = cds.qualifiers.get("locus_tag", [None])[0]

                    # Optional 'gene' name reference (not stored, but could be)
                    _gene_ref = cds.qualifiers.get("gene", [None])[0]

                    # EC number: either in EC_number or as "EC:..." inside db_xref
                    ec_number = cds.qualifiers.get("EC_number", [None])[0]
                    if ec_number is None:
                        for xref in cds.qualifiers.get("db_xref", []):
                            if xref.startswith("EC:"):
                                ec_number = xref.split(":", 1)[1]
                                break

                    # GO annotations appear as "GO:xxxxxxx" inside db_xref
                    go_list = [
                        x for x in cds.qualifiers.get("db_xref", []) if x.startswith("GO:")
                    ]
                    go_terms = ",".join(go_list) if go_list else None

                    # Convert location to simple values
                    start, end, strand, loc_text = parse_location(cds.location)

                    insert_protein_sql = text(
                        "INSERT INTO proteins "
                        "(species_id, gene_id, protein_id, product_name, ec_number, go_terms, location) "
                        "VALUES (:sid, :gid, :pid, :prod, :ec, :go, :loc)"
                    )
                    conn.execute(
                        insert_protein_sql,
                        {
                            "sid": species_id,
                            # If we did not see a matching gene, gene_id will be NULL
                            "gid": locus_to_gid.get(locus_tag),
                            "pid": protein_id,
                            "prod": product,
                            "ec": ec_number,
                            "go": go_terms,
                            "loc": loc_text,
                        },
                    )
                    protein_count += 1

                # ========== update species counters ===========================
                update_counts_sql = text(
                    "UPDATE species SET n_genes=:ng, n_proteins=:np WHERE id=:sid"
                )
                conn.execute(
                    update_counts_sql,
                    {"ng": len(genes), "np": protein_count, "sid": species_id},
                )

    # If we reach here without exceptions, everything went fine
    print("Done.")
    return 0


if __name__ == "__main__":
    # Exit with the code returned by main() (0 = success, non-zero = error)
    raise SystemExit(main())
