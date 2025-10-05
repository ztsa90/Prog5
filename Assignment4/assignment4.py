#!/usr/bin/env python3
"""
assignment4.py — Programming 5 (SQL)

Read one GenBank (.gb/.gbff[.gz]) file for Archaea and store data in MariaDB:
- species per record (name, accession+version, genome size, taxid, counts)
- genes (per record) with locus_tag, gene name, location
- proteins (CDS) with protein_id, product, location, EC and GO terms
Connection settings are read from ~/.my.cnf ([client] section).
"""

import gzip
import os
import sys
from configparser import ConfigParser

from Bio import SeqIO
from sqlalchemy import create_engine, text


# ---------------------------- DB utilities -----------------------------------
def db_url_from_mycnf(path="~/.my.cnf"):
    """Build a SQLAlchemy URL from ~/.my.cnf ([client] section)."""
    cfg = ConfigParser()
    cfg.read(os.path.expanduser(path))
    if "client" not in cfg:
        raise RuntimeError("Missing [client] section in ~/.my.cnf")

    c = cfg["client"]
    user = c.get("user")
    pwd = c.get("password")
    host = c.get("host", "mariadb.bin.bioinf.nl")
    db = c.get("database")

    if not (user and pwd and db):
        raise RuntimeError("user/password/database missing in ~/.my.cnf [client]")

    return f"mysql+pymysql://{user}:{pwd}@{host}/{db}"


def engine_from_cnf():
    """Create a SQLAlchemy engine using credentials from ~/.my.cnf."""
    url = db_url_from_mycnf()
    # pool_recycle prevents stale connections on long runs
    return create_engine(url, pool_recycle=3600)


# ------------------------------ DDL (schema) ---------------------------------
DDL_SPECIES = """
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

DDL_GENES = """
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

DDL_PROTEINS = """
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


# ------------------------------- helpers -------------------------------------
def open_any(path):
    """Open plain or .gz file as text."""
    return gzip.open(path, "rt") if path.endswith(".gz") else open(
        path, "rt", encoding="utf-8", errors="replace"
    )


def parse_location(loc):
    """Return (start, end, strand, as_text) from a Biopython location."""
    try:
        # GenBank locations are 0-based; report 1-based start for readability
        start = int(loc.start) + 1
        end = int(loc.end)
        strand = "+" if (loc.strand is None or loc.strand >= 0) else "-"
        return start, end, strand, str(loc)
    except Exception:
        # Keep something useful even if parsing fails
        return None, None, None, str(loc)


def extract_taxid(record):
    """Extract NCBI taxonomy id (integer) from source feature, if present."""
    for feat in record.features:
        if feat.type == "source":
            for x in feat.qualifiers.get("db_xref", []):
                if x.startswith("taxon:"):
                    try:
                        return int(x.split(":", 1)[1])
                    except Exception:
                        return None
    return None


# --------------------------------- main --------------------------------------
def main():
    # Input file is given on the command line (hard-coded would also be OK)
    if len(sys.argv) < 2:
        print("Usage: assignment4.py /path/to/archaea.X.genomic.gbff[.gz]")
        return 1

    gbff_path = sys.argv[1]
    if not os.path.exists(gbff_path):
        print(f"Input not found: {gbff_path}", file=sys.stderr)
        return 2

    engine = engine_from_cnf()

    # 1) Ensure tables exist (C in CRUD)
    with engine.begin() as conn:
        # Prevent server-side timeout on large inserts
        conn.execute(text("SET SESSION max_statement_time=0"))
        conn.execute(text(DDL_SPECIES))
        conn.execute(text(DDL_GENES))
        conn.execute(text(DDL_PROTEINS))

    # 2) Parse the GenBank file and insert rows
    with open_any(gbff_path) as handle:
        for record in SeqIO.parse(handle, "genbank"):
            species_name = record.annotations.get("organism")
            accession = record.id  # usually already includes the version
            genome_size = len(record.seq)
            taxid = extract_taxid(record)

            # Insert species row; get its auto-increment id
            with engine.begin() as conn:
                conn.execute(text("SET SESSION max_statement_time=0"))
                res = conn.execute(
                    text(
                        "INSERT INTO species "
                        "(name, accession, genome_size_bp, n_genes, n_proteins, taxid) "
                        "VALUES (:n, :a, :s, 0, 0, :t)"
                    ),
                    {"n": species_name, "a": accession, "s": genome_size, "t": taxid},
                )
                species_id = res.lastrowid

                # Insert genes; remember gene ids by locus_tag for CDS linking
                locus_to_gid = {}
                genes = [f for f in record.features if f.type == "gene"]
                for g in genes:
                    locus = g.qualifiers.get("locus_tag", [None])[0]
                    gname = g.qualifiers.get("gene", [None])[0]
                    start, end, strand, loc_txt = parse_location(g.location)

                    r = conn.execute(
                        text(
                            "INSERT INTO genes "
                            "(species_id, locus_tag, gene_name, location, strand, start_bp, end_bp) "
                            "VALUES (:sid, :lt, :gn, :loc, :st, :s, :e)"
                        ),
                        {
                            "sid": species_id,
                            "lt": locus,
                            "gn": gname,
                            "loc": loc_txt,
                            "st": strand,
                            "s": start,
                            "e": end,
                        },
                    )
                    locus_to_gid[locus] = r.lastrowid

                # Insert proteins (CDS)
                pcount = 0
                for p in (f for f in record.features if f.type == "CDS"):
                    prot_id = p.qualifiers.get("protein_id", [None])[0]
                    product = p.qualifiers.get("product", [None])[0]
                    locus = p.qualifiers.get("locus_tag", [None])[0]
                    # Optional gene name reference
                    _gene_ref = p.qualifiers.get("gene", [None])[0]

                    # EC number may be in EC_number or inside db_xref as "EC:..."
                    ec = p.qualifiers.get("EC_number", [None])[0]
                    if ec is None:
                        for x in p.qualifiers.get("db_xref", []):
                            if x.startswith("EC:"):
                                ec = x.split(":", 1)[1]
                                break

                    # GO terms appear in db_xref as "GO:..."
                    go_terms = [x for x in p.qualifiers.get("db_xref", []) if x.startswith("GO:")]
                    go_text = ",".join(go_terms) if go_terms else None

                    start, end, strand, loc_txt = parse_location(p.location)

                    conn.execute(
                        text(
                            "INSERT INTO proteins "
                            "(species_id, gene_id, protein_id, product_name, ec_number, go_terms, location) "
                            "VALUES (:sid, :gid, :pid, :prod, :ec, :go, :loc)"
                        ),
                        {
                            "sid": species_id,
                            "gid": locus_to_gid.get(locus),
                            "pid": prot_id,
                            "prod": product,
                            "ec": ec,
                            "go": go_text,
                            "loc": loc_txt,
                        },
                    )
                    pcount += 1

                # Update gene/protein counters on the species row (U in CRUD)
                conn.execute(
                    text("UPDATE species SET n_genes=:ng, n_proteins=:np WHERE id=:sid"),
                    {"ng": len(genes), "np": pcount, "sid": species_id},
                )

    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
