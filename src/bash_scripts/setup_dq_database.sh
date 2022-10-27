echo "Creating the database"
createdb -h 0.0.0.0 -U user -p 5432 dq_metastore

echo "Creating tables"
psql -h 0.0.0.0 -U user -p 5432 dq_metastore < create_tables.sql