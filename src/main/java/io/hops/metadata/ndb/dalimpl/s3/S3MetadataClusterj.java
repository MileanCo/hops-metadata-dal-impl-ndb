package io.hops.metadata.ndb.dalimpl.s3;

import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.s3.TablesDef;
import io.hops.metadata.s3.dal.S3MetadataAccess;
import io.hops.metadata.s3.entity.S3PathMetadata;

import java.util.List;

public class S3MetadataClusterj implements TablesDef.S3PathMetadataTableDef, S3MetadataAccess<S3PathMetadata> {

    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public S3PathMetadata getPath(String parent, String child) {
        return null;
    }

    @Override
    public boolean putPath(S3PathMetadata path) {
        return false;
    }

    @Override
    public boolean deletePath(String parent, String child) {
        return false;
    }

    @Override
    public boolean putPaths(List<S3PathMetadata> paths) {
        return false;
    }

    @Override
    public boolean deletePaths(List<List<String>> paths) {
        return false;
    }

    @Override
    public boolean isDirEmpty(String parent, String child) {
        return false;
    }

    @Override
    public boolean deleteTable(String table_name) {
        return false;
    }


    @Override
    public List<S3PathMetadata> getExpiredFiles(long modTime) {
        return null;
    }

    @Override
    public List<S3PathMetadata> getPathChildren(String parent) {
        return null;
    }
}
