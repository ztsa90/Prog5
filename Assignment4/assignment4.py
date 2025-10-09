#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
assignment4.py — Programming 5 (SQL)

Parse one Archaea GenBank file (.gb / .gbff / .gbff.gz) and store selected data
into a MariaDB database using SQLAlchemy with **safe parameterization**.

Tables created (simple 3-table design):
  - species   (name, accession, genome_size_bp, n_genes, n_proteins, taxid)
  - genes     (species_id FK, locus_tag, gene_name, start_bp, end_bp, strand, location)
  - proteins  (species_id, gene_id FKs, protein_id, product_name, EC/GO, location)

Credentials are read from ~/.my.cnf ([client] section). Never commit credentials.
"""

from __future__ import annotations

import argparse
import gzip
import logging
import os
from configparser import ConfigParser
from typing import Dict, List, Optional, Tuple

from Bio import SeqIO
import sqlalchemy

# Version-safe imports for SQLAlchemy 1.4/2.x
if sqlalchemy.__version__.startswith(("1.4", "2")):
    # pylint: disable=ungrouped-imports
    from sqlalchemy import create_engine, text  # type: ignore
    from sqlalchemy.engine import URL  # type: ignore
else:  # Fallback (kept identical for older envs)
    # pylint: disable=ungrouped-imports
    from sqlalchemy import create_engine, text  # type: ignore
    from sqlalchemy.engine import URL  # type: ignore


# ================================ Constants ===================================

DEFAULT_GBFF: str = (
    "/data/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/refseq/release/"
    "archaea/archaea.1.genomic.gbff"
)
DEFAULT_BATCH_SIZE: int = 100
DEFAULT_PROGRESS_EVERY: int = 100
LOG_FORMAT: str = "[%(levelname)s] %(message)s"

# DDL (InnoDB with FKs)
DDL_SPECIES: str = """
CREATE TABLE IF NOT EXISTS species (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255),
  accession VARCHAR(64),
  genome_size_bp INT,
  n_genes INT,
  n_proteins INT,
  taxid INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DDL_GENES: str = """
CREATE TABLE IF NOT EXISTS genes (
  id INT AUTO_INCREMENT PRIMARY KEY,
  species_id INT,
  locus_tag VARCHAR(128),
  gene_name VARCHAR(255),
  location TEXT,
  strand CHAR(1),
  start_bp INT,
  end_bp INT,
  FOREIGN KEY (species_id) REFERENCES species(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DDL_PROTEINS: str = """
CREATE TABLE IF NOT EXISTS proteins (
  id INT AUTO_INCREMENT PRIMARY KEY,
  species_id INT,
  gene_id INT,
  protein_id VARCHAR(64),
  product_name TEXT,
  ec_number VARCHAR(64),
  go_terms TEXT,
  location TEXT,
  FOREIGN KEY (species_id) REFERENCES species(id),
  FOREIGN KEY (gene_id) REFERENCES genes(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


# =============================== CLI Parsing ==================================

def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Parse an Archaea GenBank file and store data in MariaDB."
    )
    parser.add_argument(
        "gbff",
        nargs="?",
        default=None,
        help="Path to .gb/.gbff[.gz]. If omitted, a default path is used.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="How many proteins to insert per DB batch (default: 100).",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=DEFAULT_PROGRESS_EVERY,
        help="Print a progress line every N CDS scanned (default: 100).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (default: INFO).",
    )
    return parser.parse_args()


# ============================ Database Connection =============================

def read_mycnf_url(path: str = "~/.my.cnf") -> URL:
    """
    Read ~/.my.cnf and build a SQLAlchemy URL.

    Expected section:
      [client]
      user=...
      password=...
      host=mariadb.bin.bioinf.nl
      database=YourDB
      # (optional) port=3306
    """
    config = ConfigParser()
    expanded = os.path.expanduser(path)
    config.read(expanded)

    if "client" not in config:
        raise RuntimeError("Missing [client] section in ~/.my.cnf")

    section = config["client"]
    user = section.get("user")
    password = section.get("password")
    host = section.get("host", fallback="mariadb.bin.bioinf.nl")
    port = section.getint("port", fallback=3306)
    database = section.get("database")

    if not user or not password or not database:
        raise RuntimeError("user/password/database missing in ~/.my.cnf [client]")

    driver = "mysql+mysqldb"
    try:
        import MySQLdb  # type: ignore  # noqa: F401
    except Exception:  # pylint: disable=broad-except
        driver = "mysql+pymysql"

    return URL.create(
        drivername=driver,
        username=user,
        password=password,
        host=host,
        port=port,
        database=database,
    )


def make_engine():
    """
    Create a SQLAlchemy engine with safe defaults:
      - pool_pre_ping=True to avoid 'server has gone away'
      - pool_recycle to refresh stale TCP connections
      - future=True unified 1.4/2.x API
      - init max_statement_time=0 to prevent timeouts on large batches
    """
    url = read_mycnf_url()
    return create_engine(
        url,
        pool_recycle=3600,
        pool_pre_ping=True,
        future=True,
        connect_args={"init_command": "SET SESSION max_statement_time=0"},
    )


def set_no_timeout(conn) -> None:
    """Try to disable statement timeouts for long jobs (ignore if not supported)."""
    try:
        conn.execute(text("SET SESSION max_statement_time=0"))
    except Exception:  # pylint: disable=broad-except
        logging.debug("max_statement_time not available; continuing.")


def last_insert_id(conn) -> int:
    """Get the last auto-increment value for the session (driver-safe)."""
    return int(conn.execute(text("SELECT LAST_INSERT_ID()")).scalar_one())


# ================================ IO Helpers ==================================

def open_any(path: str):
    """Open a plain text file or a .gz file in text mode with UTF-8."""
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return open(path, "rt", encoding="utf-8", errors="replace")


def parse_location(loc) -> Tuple[Optional[int], Optional[int], Optional[str], str]:
    """
    Convert a Biopython location object to simple fields.

    Returns:
      start_1_based, end, strand_symbol ('+'|'-'|None), original_text
    """
    try:
        start_1 = int(loc.start) + 1
        end = int(loc.end)
        strand_symbol = "+" if (loc.strand is None or loc.strand >= 0) else "-"
        return start_1, end, strand_symbol, str(loc)
    except Exception:  # pylint: disable=broad-except
        return None, None, None, str(loc)


def extract_taxid(record) -> Optional[int]:
    """Find 'taxon:XXXX' under the 'source' feature's db_xref entries."""
    for feature in record.features:
        if feature.type != "source":
            continue
        for ref in feature.qualifiers.get("db_xref", []):
            if ref.startswith("taxon:"):
                try:
                    return int(ref.split(":", maxsplit=1)[1])
                except Exception:  # pylint: disable=broad-except
                    return None
    return None


# ================================ Insertion ===================================

def create_tables(engine) -> None:
    """Create species, genes, proteins tables if they do not exist."""
    with engine.begin() as conn:
        set_no_timeout(conn)
        conn.execute(text(DDL_SPECIES))
        conn.execute(text(DDL_GENES))
        conn.execute(text(DDL_PROTEINS))


def insert_species(
    conn,
    name: str,
    accession: str,
    genome_size: int,
    taxid: Optional[int],
) -> int:
    """Insert one species row and return its new ID."""
    res = conn.execute(
        text(
            """
            INSERT INTO species
              (name, accession, genome_size_bp, n_genes, n_proteins, taxid)
            VALUES
              (:n, :a, :s, 0, 0, :t)
            """
        ),
        {"n": name, "a": accession, "s": genome_size, "t": taxid},
    )
    return getattr(res, "lastrowid", None) or last_insert_id(conn)


def insert_gene(
    conn,
    species_id: int,
    locus_tag: Optional[str],
    gene_name: Optional[str],
    location_text: str,
    strand: Optional[str],
    start_bp: Optional[int],
    end_bp: Optional[int],
) -> int:
    """Insert one gene and return its ID."""
    res = conn.execute(
        text(
            """
            INSERT INTO genes
              (species_id, locus_tag, gene_name, location, strand, start_bp, end_bp)
            VALUES
              (:sid, :lt, :gn, :loc, :st, :s, :e)
            """
        ),
        {
            "sid": species_id,
            "lt": locus_tag,
            "gn": gene_name,
            "loc": location_text,
            "st": strand,
            "s": start_bp,
            "e": end_bp,
        },
    )
    return getattr(res, "lastrowid", None) or last_insert_id(conn)


def batch_insert_proteins(conn, rows: List[Dict[str, Optional[str]]]) -> int:
    """
    Insert a batch of proteins using executemany with bound parameters.
    Returns number of rows attempted (driver may report affected differently).
    """
    if not rows:
        return 0

    conn.execute(
        text(
            """
            INSERT INTO proteins
              (species_id, gene_id, protein_id, product_name, ec_number, go_terms, location)
            VALUES
              (:sid, :gid, :pid, :prod, :ec, :go, :loc)
            """
        ),
        rows,
    )
    return len(rows)


def update_species_counts(conn, species_id: int, n_genes: int, n_proteins: int) -> None:
    """Update cached counts on the species row."""
    conn.execute(
        text("UPDATE species SET n_genes=:ng, n_proteins=:np WHERE id=:sid"),
        {"ng": n_genes, "np": n_proteins, "sid": species_id},
    )


# ================================== Main ======================================

def process_record(
    conn,
    record,
    batch_size: int,
    progress_every: int,
) -> Tuple[int, int]:
    """
    Insert one SeqRecord into species/genes/proteins.

    Returns:
      (genes_inserted, proteins_inserted)
    """
    # Species meta
    species_name = record.annotations.get("organism", "unknown")
    accession = record.id  # NOTE: this is accession; some releases include version in .id
    genome_size = len(record.seq)
    taxid = extract_taxid(record)

    species_id = insert_species(
        conn,
        name=species_name,
        accession=accession,
        genome_size=genome_size,
        taxid=taxid,
    )

    # Genes (need individual inserts to capture gene_id for proteins FK)
    locus_to_geneid: Dict[str, int] = {}
    gene_features = [ftr for ftr in record.features if ftr.type == "gene"]
    for gene in gene_features:
        locus_tag = gene.qualifiers.get("locus_tag", [None])[0]
        gene_name = gene.qualifiers.get("gene", [None])[0]
        start_bp, end_bp, strand, loc_text = parse_location(gene.location)

        gene_id = insert_gene(
            conn=conn,
            species_id=species_id,
            locus_tag=locus_tag,
            gene_name=gene_name,
            location_text=loc_text,
            strand=strand,
            start_bp=start_bp,
            end_bp=end_bp,
        )
        if locus_tag:
            locus_to_geneid[locus_tag] = gene_id

    # Proteins (batch)
    protein_batch: List[Dict[str, Optional[str]]] = []
    proteins_inserted = 0

    for idx, cds in enumerate((f for f in record.features if f.type == "CDS"), start=1):
        protein_id = cds.qualifiers.get("protein_id", [None])[0]
        product = cds.qualifiers.get("product", [None])[0]
        locus_tag = cds.qualifiers.get("locus_tag", [None])[0]

        # EC can be in EC_number or sometimes in db_xref as "EC:xxxx"
        ec = cds.qualifiers.get("EC_number", [None])[0]
        if ec is None:
            for ref in cds.qualifiers.get("db_xref", []):
                if ref.startswith("EC:"):
                    ec = ref.split(":", maxsplit=1)[1]
                    break

        # GO terms are often present in db_xref as "GO:0000000"
        go_list = [x for x in cds.qualifiers.get("db_xref", []) if x.startswith("GO:")]
        go_terms = ";".join(go_list) if go_list else None

        start_bp, end_bp, strand, loc_text = parse_location(cds.location)

        protein_batch.append(
            {
                "sid": species_id,
                "gid": locus_to_geneid.get(locus_tag),
                "pid": protein_id,
                "prod": product,
                "ec": ec,
                "go": go_terms,
                "loc": loc_text,
            }
        )

        if idx % progress_every == 0:
            logging.info(
                "Scanning CDS %d for species=%s accession=%s",
                idx,
                species_name,
                accession,
            )

        if len(protein_batch) >= batch_size:
            proteins_inserted += batch_insert_proteins(conn, protein_batch)
            protein_batch.clear()
            logging.info(
                "Inserted %d proteins so far for species=%s accession=%s",
                proteins_inserted,
                species_name,
                accession,
            )

    if protein_batch:
        proteins_inserted += batch_insert_proteins(conn, protein_batch)
        protein_batch.clear()
        logging.info(
            "Inserted %d proteins (final) for species=%s accession=%s",
            proteins_inserted,
            species_name,
            accession,
        )

    # Cache counts on species row (nice for quick queries later)
    update_species_counts(
        conn,
        species_id=species_id,
        n_genes=len(gene_features),
        n_proteins=proteins_inserted,
    )

    logging.info(
        "[record] %s (%s) | genes=%d | proteins=%d",
        species_name,
        accession,
        len(gene_features),
        proteins_inserted,
    )

    return len(gene_features), proteins_inserted


def main() -> int:
    """Entry point: validate input, setup DB, stream parse, batch insert."""
    args = parse_args()
    logging.basicConfig(level=args.log_level, format=LOG_FORMAT)

    gbff_path = args.gbff or DEFAULT_GBFF
    if not os.path.exists(gbff_path):
        logging.error("Input not found: %s", gbff_path)
        return 2

    engine = make_engine()
    create_tables(engine)

    total_records = 0
    total_genes = 0
    total_proteins = 0

    with open_any(gbff_path) as handle:
        for record in SeqIO.parse(handle, "genbank"):
            total_records += 1
            # one transaction per record (safer + keeps memory bounded)
            with engine.begin() as conn:
                set_no_timeout(conn)
                g_count, p_count = process_record(
                    conn=conn,
                    record=record,
                    batch_size=args.batch_size,
                    progress_every=args.progress_every,
                )
            total_genes += g_count
            total_proteins += p_count

    logging.info("=== SUMMARY ===")
    logging.info("records (samples): %d", total_records)
    logging.info("total genes      : %d", total_genes)
    logging.info("total proteins   : %d", total_proteins)
    logging.info("Done. All records inserted successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
