#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
assignment4.py — Programming 5 (SQL)

This program reads one Archaea GenBank file (.gb / .gbff / .gbff.gz)
and stores selected data into a MariaDB database.

It saves three kinds of information:
1) species   → name, accession, genome size, gene count, protein count, taxid
2) genes     → locus_tag, gene name, start, end, strand, location text
3) proteins  → protein_id, product name, EC number, GO terms, location, link to gene

Database credentials are read from ~/.my.cnf in the [client] section:
[client]
user=USERNAME
password=PASSWORD
host=mariadb.bin.bioinf.nl
database=DATABASE

Notes:
- We use SQLAlchemy and work with text() queries for clarity and stability.
- We support SQLAlchemy 1.4 and 2.x.
- We prefer the MySQLdb driver; we fall back to PyMySQL when needed.
- We insert proteins in batches and print progress, so the job looks alive.
"""

from __future__ import annotations  # For forward type references on older Python

# ============================= Standard Library ================================
import argparse  # For command-line options
import gzip      # To open .gz files as text
import logging   # For progress and debug messages
import os        # For paths and environment
import sys       # For exit codes and argv
from configparser import ConfigParser  # For reading ~/.my.cnf
from typing import Dict, Iterable, List, Optional, Tuple  # For type hints

# ============================ Third-party Libraries ============================
from Bio import SeqIO  # Biopython: to parse GenBank files
import sqlalchemy      # Core package; we branch on version for imports

# SQLAlchemy version-safe imports
if sqlalchemy.__version__.startswith("1.4"):
    # SQLAlchemy 1.4
    from sqlalchemy import create_engine, text  # type: ignore
    from sqlalchemy.engine import URL  # type: ignore
else:
    # SQLAlchemy 2.x and other future versions
    from sqlalchemy import create_engine, text  # type: ignore
    from sqlalchemy.engine import URL  # type: ignore

# ================================ Constants ===================================
DEFAULT_GBFF: str = (
    "/data/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/"
    "refseq/release/archaea/archaea.1.genomic.gbff"
)
DEFAULT_BATCH_SIZE: int = 100        # How many proteins per DB batch insert
DEFAULT_PROGRESS_EVERY: int = 100    # How often to print scan progress (CDS count)
LOG_FORMAT: str = "[%(levelname)s] %(message)s"  # Simple log format

# Table DDL statements (InnoDB with foreign keys)
DDL_SPECIES: str = """
CREATE TABLE IF NOT EXISTS species (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255),
  accession VARCHAR(64),
  genome_size_bp INT,
  n_genes INT,
  n_proteins INT,
  taxid INT
) ENGINE=InnoDB;
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
) ENGINE=InnoDB;
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
) ENGINE=InnoDB;
"""


# =============================== CLI Parsing ==================================
def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """
    Parse command-line arguments.

    Parameters
    ----------
    argv : Optional[List[str]]
        A list of arguments. If None, uses sys.argv.

    Returns
    -------
    argparse.Namespace
        Parsed options.
    """
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
    return parser.parse_args(argv)


# ============================ Database Connection =============================
def read_mycnf_url(path: str = "~/.my.cnf") -> URL:
    """
    Read ~/.my.cnf and build a SQLAlchemy URL.

    We prefer MySQLdb (mysqlclient). If it is not available, we use PyMySQL.

    Parameters
    ----------
    path : str
        Path to the configuration file.

    Returns
    -------
    URL
        A SQLAlchemy URL object.
    """
    config = ConfigParser()
    expanded = os.path.expanduser(path)
    config.read(expanded)

    if "client" not in config:
        raise RuntimeError("Missing [client] section in ~/.my.cnf")

    section = config["client"]
    user = section.get("user", fallback=None)
    password = section.get("password", fallback=None)
    host = section.get("host", fallback="mariadb.bin.bioinf.nl")
    database = section.get("database", fallback=None)

    if not user or not password or not database:
        raise RuntimeError("user/password/database missing in ~/.my.cnf [client]")

    # Try MySQLdb first, then fall back to PyMySQL.
    driver = "mysql+mysqldb"
    try:
        __import__("MySQLdb")
    except Exception:  # pylint: disable=broad-except
        driver = "mysql+pymysql"

    # Build URL; URL.create handles special characters safely.
    return URL.create(
        drivername=driver,
        username=user,
        password=password,
        host=host,
        database=database,
    )


def make_engine():
    """
    Create a SQLAlchemy engine with safe defaults.

    We set pool_pre_ping to avoid 'server has gone away' errors.
    We also set pool_recycle to refresh stale connections.

    Returns
    -------
    sqlalchemy.Engine
        A ready-to-use Engine.
    """
    url = read_mycnf_url()
    # future=True is good for SQLAlchemy 1.4+ unified API
    return create_engine(url, pool_recycle=3600, pool_pre_ping=True, future=True)


def set_no_timeout(conn) -> None:
    """
    Try to disable statement timeouts for long jobs.

    Some MariaDB setups do not expose this variable.
    We catch errors and continue.

    Parameters
    ----------
    conn :
        An active SQLAlchemy Connection (within a transaction).
    """
    try:
        conn.execute(text("SET SESSION max_statement_time=0"))
    except Exception:  # pylint: disable=broad-except
        # Not critical; continue without changing the session setting
        logging.debug("max_statement_time not available; continuing.")


def last_insert_id(conn) -> int:
    """
    Retrieve the last auto-increment value from the current session.

    This is reliable with text() INSERTs across SQLAlchemy 1.4/2.x.

    Parameters
    ----------
    conn :
        An active SQLAlchemy Connection (within a transaction).

    Returns
    -------
    int
        The last inserted id in this session.
    """
    return int(conn.execute(text("SELECT LAST_INSERT_ID()")).scalar_one())


# ================================ IO Helpers ==================================
def open_any(path: str):
    """
    Open a text file or a .gz file as a text stream.

    Parameters
    ----------
    path : str
        The file path.

    Returns
    -------
    IO
        A text stream that can be read line by line.
    """
    if path.endswith(".gz"):
        # "rt" gives text mode; gzip handles decompression
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return open(path, "rt", encoding="utf-8", errors="replace")


def parse_location(loc) -> Tuple[Optional[int], Optional[int], Optional[str], str]:
    """
    Convert a Biopython location object to simple fields.

    We return 1-based start, end, strand symbol, and the string form.

    Parameters
    ----------
    loc :
        A Biopython FeatureLocation or CompoundLocation.

    Returns
    -------
    Tuple[Optional[int], Optional[int], Optional[str], str]
        (start_1_based, end, strand_symbol, location_text)
    """
    try:
        start_1 = int(loc.start) + 1
        end = int(loc.end)
        strand_symbol = "+" if (loc.strand is None or loc.strand >= 0) else "-"
        return start_1, end, strand_symbol, str(loc)
    except Exception:  # pylint: disable=broad-except
        # If location is complex, we still return the text
        return None, None, None, str(loc)


def extract_taxid(record) -> Optional[int]:
    """
    Get the NCBI taxonomy id from the 'source' feature.

    We look for db_xref entries like 'taxon:XXXX'.

    Parameters
    ----------
    record :
        A Biopython SeqRecord.

    Returns
    -------
    Optional[int]
        The taxid as int, or None if not found.
    """
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
    """
    Create the three tables if they do not exist.

    Parameters
    ----------
    engine :
        The SQLAlchemy Engine to use.
    """
    with engine.begin() as conn:
        set_no_timeout(conn)
        conn.execute(text(DDL_SPECIES))
        conn.execute(text(DDL_GENES))
        conn.execute(text(DDL_PROTEINS))


def insert_species(conn, name: str, accession: str,
                   genome_size: int, taxid: Optional[int]) -> int:
    """
    Insert one species row and return its primary key id.

    Parameters
    ----------
    conn :
        Active SQLAlchemy Connection.
    name : str
        Species name.
    accession : str
        Genome accession (with version).
    genome_size : int
        Genome size in base pairs.
    taxid : Optional[int]
        NCBI taxonomy id.

    Returns
    -------
    int
        The species.id value.
    """
    res = conn.execute(
        text(
            """
            INSERT INTO species
            (name, accession, genome_size_bp, n_genes, n_proteins, taxid)
            VALUES (:n, :a, :s, 0, 0, :t)
            """
        ),
        {"n": name, "a": accession, "s": genome_size, "t": taxid},
    )
    # Some drivers do not return .lastrowid here; we fetch it safely.
    return getattr(res, "lastrowid", None) or last_insert_id(conn)


def insert_gene(conn, species_id: int, locus_tag: Optional[str],
                gene_name: Optional[str], location_text: str,
                strand: Optional[str], start_bp: Optional[int],
                end_bp: Optional[int]) -> int:
    """
    Insert one gene and return its id.

    Parameters
    ----------
    conn :
        Active SQLAlchemy Connection.
    species_id : int
        The parent species id.
    locus_tag : Optional[str]
        Gene locus tag.
    gene_name : Optional[str]
        Gene symbol or name.
    location_text : str
        The location string as shown in GenBank.
    strand : Optional[str]
        '+' or '-' or None.
    start_bp : Optional[int]
        1-based start coordinate.
    end_bp : Optional[int]
        End coordinate.

    Returns
    -------
    int
        The genes.id value.
    """
    res = conn.execute(
        text(
            """
            INSERT INTO genes
            (species_id, locus_tag, gene_name, location, strand, start_bp, end_bp)
            VALUES (:sid, :lt, :gn, :loc, :st, :s, :e)
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
    Insert a batch of proteins using executemany.

    Parameters
    ----------
    conn :
        Active SQLAlchemy Connection.
    rows : List[Dict[str, Optional[str]]]
        Each item holds bound parameters for one protein.

    Returns
    -------
    int
        Number of inserted rows (same as len(rows)).
    """
    if not rows:
        return 0

    conn.execute(
        text(
            """
            INSERT INTO proteins
            (species_id, gene_id, protein_id, product_name, ec_number, go_terms, location)
            VALUES (:sid, :gid, :pid, :prod, :ec, :go, :loc)
            """
        ),
        rows,
    )
    return len(rows)


def update_species_counts(conn, species_id: int,
                          n_genes: int, n_proteins: int) -> None:
    """
    Update cached counts in the species row.

    Parameters
    ----------
    conn :
        Active SQLAlchemy Connection.
    species_id : int
        The species to update.
    n_genes : int
        Count of gene features.
    n_proteins : int
        Count of CDS/proteins inserted.
    """
    conn.execute(
        text("UPDATE species SET n_genes=:ng, n_proteins=:np WHERE id=:sid"),
        {"ng": n_genes, "np": n_proteins, "sid": species_id},
    )


# ================================== Main ======================================
def process_record(conn, record, batch_size: int,
                   progress_every: int) -> Tuple[int, int]:
    """
    Insert one SeqRecord into the three tables.

    Parameters
    ----------
    conn :
        Active SQLAlchemy Connection.
    record :
        Biopython SeqRecord.
    batch_size : int
        Protein batch size for DB flush.
    progress_every : int
        Print a progress log every N CDS visited.

    Returns
    -------
    Tuple[int, int]
        (genes_inserted, proteins_inserted)
    """
    # Read high-level fields for species
    species_name = record.annotations.get("organism")
    accession = record.id
    genome_size = len(record.seq)
    taxid = extract_taxid(record)

    # Insert one species
    species_id = insert_species(
        conn,
        name=species_name,
        accession=accession,
        genome_size=genome_size,
        taxid=taxid,
    )

    # Insert genes one by one, because we need each new gene id
    locus_to_geneid: Dict[str, int] = {}
    genes = [f for f in record.features if f.type == "gene"]
    for gene in genes:
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
            # Only map real locus tags
            locus_to_geneid[locus_tag] = gene_id

    # Prepare batch buffer for CDS → proteins
    protein_batch: List[Dict[str, Optional[str]]] = []
    proteins_inserted = 0

    # Iterate over CDS features; add to batch; flush when full
    for i, cds in enumerate((f for f in record.features if f.type == "CDS"), start=1):
        protein_id = cds.qualifiers.get("protein_id", [None])[0]
        product = cds.qualifiers.get("product", [None])[0]
        locus_tag = cds.qualifiers.get("locus_tag", [None])[0]

        # EC number may be under EC_number or under db_xref as "EC:xxxx"
        ec = cds.qualifiers.get("EC_number", [None])[0]
        if ec is None:
            for ref in cds.qualifiers.get("db_xref", []):
                if ref.startswith("EC:"):
                    ec = ref.split(":", maxsplit=1)[1]
                    break

        # GO terms are in db_xref as "GO:xxxxxxx"
        go_list = [x for x in cds.qualifiers.get("db_xref", []) if x.startswith("GO:")]
        go_terms = ",".join(go_list) if go_list else None

        # Location text and coordinates
        _start, _end, _strand, loc_text = parse_location(cds.location)

        # Build one row for executemany
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

        # Periodic scan progress (even if the batch is not flushed yet)
        if i % progress_every == 0:
            logging.info(
                "Scanning CDS %d for species=%s accession=%s",
                i,
                species_name,
                accession,
            )

        # Flush when batch is full
        if len(protein_batch) >= batch_size:
            added = batch_insert_proteins(conn, protein_batch)
            proteins_inserted += added
            protein_batch.clear()
            logging.info(
                "Inserted %d proteins so far for species=%s accession=%s",
                proteins_inserted,
                species_name,
                accession,
            )

    # Flush final partial batch
    if protein_batch:
        added = batch_insert_proteins(conn, protein_batch)
        proteins_inserted += added
        protein_batch.clear()
        logging.info(
            "Inserted %d proteins (final) for species=%s accession=%s",
            proteins_inserted,
            species_name,
            accession,
        )

    # Update cached counts on species
    update_species_counts(conn, species_id, n_genes=len(genes), n_proteins=proteins_inserted)

    # Summary for this record
    logging.info(
        "[record] %s (%s) | genes=%d | proteins=%d",
        species_name,
        accession,
        len(genes),
        proteins_inserted,
    )

    return len(genes), proteins_inserted


def main() -> int:
    """
    Entry point. Validate input, set up DB, parse file, and insert rows.

    Returns
    -------
    int
        Exit status code (0 for success).
    """
    args = parse_args()
    logging.basicConfig(level=args.log_level, format=LOG_FORMAT)

    # Choose input path: CLI value or default path from assignment
    gbff_path = args.gbff or DEFAULT_GBFF

    if not os.path.exists(gbff_path):
        logging.error("Input not found: %s", gbff_path)
        return 2

    # Build DB engine and create tables
    engine = make_engine()
    create_tables(engine)

    # Global counters across all records in the file
    total_records = 0
    total_genes = 0
    total_proteins = 0

    # Parse the GenBank file and stream records
    with open_any(gbff_path) as handle:
        for record in SeqIO.parse(handle, "genbank"):
            total_records += 1
            # Each record (contig/chromosome) runs in its own transaction
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

    # Final summary at INFO level
    logging.info("=== SUMMARY ===")
    logging.info("records (samples): %d", total_records)
    logging.info("total genes      : %d", total_genes)
    logging.info("total proteins   : %d", total_proteins)
    logging.info("Done. All records inserted successfully.")

    return 0


if __name__ == "__main__":
    # SystemExit allows shell to see the right return code.
    raise SystemExit(main())
