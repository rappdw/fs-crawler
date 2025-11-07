QUICK START: CSV to SQLite Migration
======================================

If you have old crawl data in CSV format from before v0.3.0, use this script
to convert it to the new SQLite database format.

USAGE:
------
python migrate_csv_to_db.py <directory> <basename>

EXAMPLE:
--------
python migrate_csv_to_db.py ./output my_crawl

This will:
  - Read: ./output/my_crawl.*.csv files
  - Create: ./output/my_crawl.db
  - Leave original CSV files unchanged

For detailed documentation, see MIGRATION.md
