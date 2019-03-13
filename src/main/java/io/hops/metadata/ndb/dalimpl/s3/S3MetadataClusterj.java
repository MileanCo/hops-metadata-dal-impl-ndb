package io.hops.metadata.ndb.dalimpl.s3;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.NdbBoolean;
import io.hops.metadata.ndb.wrapper.*;
import io.hops.metadata.s3.TablesDef;
import io.hops.metadata.s3.dal.S3MetadataAccess;
import io.hops.metadata.s3.entity.S3PathMetadata;

import java.util.ArrayList;
import java.util.List;

public class S3MetadataClusterj implements TablesDef.S3PathMetadataTableDef, S3MetadataAccess<S3PathMetadata> {
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @PersistenceCapable(table = TABLE_NAME)
    public interface S3PathMetadataDTO {
        @PrimaryKey
        @Column(name = PARENT)
        String getParent();
        void setParent(String parent);

        @PrimaryKey
        @Column(name = CHILD)
        String getChild();
        void setChild(String child);

        @Column(name = BUCKET)
        String getBucket();
        void setBucket(String bucket);

        @Column(name = IS_DELETED)
        byte getIsDeleted();
        void setIsDeleted(byte deleted);

        @Column(name = BLOCK_SIZE)
        Long getBlockSize();
        void setBlockSize(Long blockSize);

        @Column(name = FILE_LENGTH)
        Long getFileLength();
        void setFileLength(Long fileLength);

        @Column(name = MOD_TIME)
        Long getModTime();
        void setModTime(Long modTime);

        @Column(name = IS_DIR)
        byte getIsDir();
        void setIsDir(byte isDir);

        @Column(name = TABLE_CREATED)
        Long getTableCreated();
        void setTableCreated(Long tableCreated);

        @Column(name = TABLE_VERSION)
        Long getTableVersion();
        void setTableVersion(Long tableVersion);
    }

    @Override
    public S3PathMetadata getPath(String parent, String child) throws StorageException {
        HopsSession session = connector.obtainSession();
        HopsQueryBuilder qb = session.getQueryBuilder();

        HopsQueryDomainType<S3PathMetadataDTO> dobj =  qb.createQueryDefinition (S3PathMetadataDTO.class);
        dobj.where(dobj.get("parent").equal(dobj.param("parent_param")));
        dobj.where(dobj.get("child").equal(dobj.param("child_param")));

        HopsQuery<S3PathMetadataDTO> query = session.createQuery(dobj);
        query.setParameter("parent_param", parent);
        query.setParameter("child_param", child);
        List<S3PathMetadataDTO> results = query.getResultList();

        S3PathMetadata path = null;
        if (results.size() == 1) {
            S3PathMetadataDTO res = results.get(0);
            path = new S3PathMetadata(
                    res.getParent(),
                    res.getChild(),
                    res.getBucket(),
                    NdbBoolean.convert(res.getIsDeleted()),
                    res.getBlockSize(),
                    res.getFileLength(),
                    res.getModTime(),
                    NdbBoolean.convert(res.getIsDir()),
                    res.getTableCreated(),
                    res.getTableVersion()
            );
        }
        session.release(results);
        return path;

    }

    @Override
    public boolean putPath(S3PathMetadata path) throws StorageException {
        HopsSession session = connector.obtainSession();
        S3PathMetadataDTO dto = null;

        // Check if path exists and delete if so
        S3PathMetadata existing_path = getPath(path.getParent(), path.getChild());
        if (existing_path != null) {
            deletePath(path.getParent(), path.getChild());
        }

        try {
            dto = session.newInstance(S3PathMetadataDTO.class);

            dto.setParent(path.getParent());
            dto.setChild(path.getChild());
            dto.setBucket(path.getBucket());
            dto.setBlockSize(path.getBlockSize());
            dto.setFileLength(path.getFileLength());
            dto.setIsDeleted(NdbBoolean.convert(path.isDeleted()));
            dto.setIsDir(NdbBoolean.convert(path.isDir()));
            dto.setModTime(path.getModTime());


            session.makePersistent(dto);
        } finally {
            session.release(dto);
        }
        return true;
    }

    @Override
    public boolean deletePath(String parent, String child) throws StorageException {
        HopsSession session = connector.obtainSession();
        S3PathMetadataDTO dto = null;
        try {
            dto = session.newInstance(S3PathMetadataDTO.class);
            dto.setParent(dto.getParent());
            dto.setChild(dto.getChild());
            session.deletePersistent(dto);
        } finally {
            session.release(dto);
        }
        return dto == null ? false : true;
    }

    @Override
    public boolean putPaths(List<S3PathMetadata> paths)throws StorageException  {
        boolean success = true;
        while (paths.iterator().hasNext()) {
            success &= putPath(paths.iterator().next());
        }
        return success;
    }

    @Override
    public boolean deletePaths(List<List<String>> paths)throws StorageException  {
        return false;
    }

    @Override
    public boolean isDirEmpty(String parent, String child) throws StorageException {
        String dir_key = parent + '/' + child;
        List<S3PathMetadata> dir_children = getPathChildren(dir_key);
        return dir_children.isEmpty();
    }

    /**
     * Convert a list of S3MetadataDTO's into storages
     */
    private List<S3PathMetadata> convertAndRelease(HopsSession session, List<S3PathMetadataDTO> dtos) throws StorageException {
        ArrayList<S3PathMetadata> list = new ArrayList(dtos.size());

        for (S3PathMetadataDTO dto : dtos) {
            S3PathMetadata path = new S3PathMetadata();
            path.parent = dto.getParent();
            path.child = dto.getChild();
            path.isDeleted = NdbBoolean.convert(dto.getIsDeleted());
            path.blockSize = dto.getBlockSize();
            path.fileLength = dto.getFileLength();
            path.modTime = dto.getModTime();
            path.isDir = NdbBoolean.convert(dto.getIsDir());
            path.tableCreated = dto.getTableCreated();
            path.tableVersion = dto.getTableVersion();

            list.add(path);
            session.release(dto);
        }

        return list;
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
    public List<S3PathMetadata> getPathChildren(String parent) throws StorageException {
        HopsSession session = connector.obtainSession();
        HopsQueryBuilder qb = session.getQueryBuilder();

        // build the SQL query
        HopsQueryDomainType<S3PathMetadataDTO> dobj = qb.createQueryDefinition(S3PathMetadataDTO.class);
        HopsPredicate pred1 = dobj.get("parent").equal(dobj.param("parent"));
        dobj.where(pred1);

        // Set the query search parameters
        HopsQuery<S3PathMetadataDTO> query = session.createQuery(dobj);
        query.setParameter("parent", parent);

        return convertAndRelease(session, query.getResultList());
    }
}
