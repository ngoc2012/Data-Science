REVOKE ALL PRIVILEGES ON DATABASE piscineds FROM "minh-ngu";
REASSIGN OWNED BY "minh-ngu" TO postgres;
DROP OWNED BY "minh-ngu";
DROP ROLE "minh-ngu";
