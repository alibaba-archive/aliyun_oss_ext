
CREATE OR REPLACE FUNCTION pg_catalog.read_from_oss() RETURNS integer AS '$libdir/oss_ext.so', 'oss_import' LANGUAGE C STABLE;
CREATE OR REPLACE FUNCTION pg_catalog.write_to_oss()  RETURNS integer AS '$libdir/oss_ext.so', 'oss_export' LANGUAGE C STABLE;

-- declare the protocol name along with in/out funcs
CREATE TRUSTED PROTOCOL oss (
    readfunc  = pg_catalog.read_from_oss, 
    writefunc = pg_catalog.write_to_oss
);

GRANT ALL ON PROTOCOL oss TO PUBLIC;
