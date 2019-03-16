package io.hops.metadata.ndb.dalimpl.s3;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.NdbBoolean;
import io.hops.metadata.ndb.wrapper.*;
import io.hops.metadata.s3.TablesDef;
import io.hops.metadata.s3.dal.S3PathMetaDataAccess;
import io.hops.metadata.s3.entity.S3PathMeta;

import java.util.ArrayList;
import java.util.List;

public class S3PathMetaClusterj implements TablesDef.S3PathMetadataTableDef, S3PathMetaDataAccess<S3PathMeta> {
    private ClusterjConnector connector = ClusterjConnector.getInstance();

    @PersistenceCapable(table = TABLE_NAME)
    public interface S3PathMetaDTO {
        @PrimaryKey
        @Column(name = PARENT)
        String getParent();
        void setParent(String parent);

        @PrimaryKey
        @Column(name = CHILD)
        String getChild();
        void setChild(String child);

        @PrimaryKey
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
    public S3PathMeta getPath(String parent, String child, String bucket) throws StorageException {
        HopsSession session = connector.obtainSession();
        HopsQueryBuilder qb = session.getQueryBuilder();

//        HopsQueryDomainType<S3PathMetaDTO> qdt =  qb.createQueryDefinition (S3PathMetaDTO.class);
//        HopsPredicate pred1 = qdt.get("parent").equal(qdt.param("parent_param"));
//        HopsPredicate pred2 = qdt.get("child").equal(qdt.param("child_param"));
//        qdt.where(pred1.and(pred2));
//
//        HopsQuery<S3PathMetaDTO> query = session.createQuery(qdt);
//        query.setParameter("parent_param", parent);
//        query.setParameter("child_param", child);
//        List<S3PathMetaDTO> results = query.getResultList();

        S3PathMetaDTO dto = session.find(S3PathMetaDTO.class, new Object[]{parent, child});
        if (dto == null) {
            return null;
        }
        S3PathMeta path = new S3PathMeta(
                dto.getParent(),
                dto.getChild(),
                dto.getBucket(),
                NdbBoolean.convert(dto.getIsDeleted()),
                NdbBoolean.convert(dto.getIsDir()),
                dto.getBlockSize(),
                dto.getFileLength(),
                dto.getModTime()
//                    dto.getTableCreated(),
//                    dto.getTableVersion()
        );
        session.release(dto);
        return path;

    }

    @Override
    public void putPath(S3PathMeta path) throws StorageException {
        HopsSession session = connector.obtainSession();
        S3PathMetaDTO dto = null;

//        // Check if path exists and delete if so
        // if you want to remove and add in the same transaction (which you should probably not) you should have a flush
        // between the remove and the make persistent otherwise you don't have any guaranty about the order in which the operations will be executed.
//        S3PathMeta existing_path = getPath(path.getParent(), path.getChild());
//        if (existing_path != null) {
//            deletePath(path.getParent(), path.getChild());
//        }

        try {
            dto = getDTOFromPath(session, path);
            session.savePersistent(dto);
        } finally {
            session.release(dto);
        }
    }

    private S3PathMetaDTO getDTOFromPath(HopsSession session, S3PathMeta path) throws StorageException {
        S3PathMetaDTO dto = session.newInstance(S3PathMetaDTO.class);
        dto.setParent(path.getParent());
        dto.setChild(path.getChild());
        dto.setBucket(path.getBucket());
        dto.setBlockSize(path.getBlockSize());
        dto.setFileLength(path.getFileLength());
        dto.setIsDeleted(NdbBoolean.convert(path.isDeleted()));
        dto.setIsDir(NdbBoolean.convert(path.isDir()));
        dto.setModTime(path.getModTime());
        return dto;
    }

    @Override
    public void deletePath(String parent, String child, String bucket) throws StorageException {
        HopsSession session = connector.obtainSession();
        S3PathMetaDTO dto = null;
        try {
            dto = session.newInstance(S3PathMetaDTO.class);
            dto.setParent(dto.getParent());
            dto.setChild(dto.getChild());
            session.deletePersistent(dto);
        } finally {
            session.release(dto);
        }
    }

    @Override
    public void putPaths(List<S3PathMeta> paths) throws StorageException  {
        HopsSession session = connector.obtainSession();
        List<S3PathMetaDTO> path_dtos = new ArrayList<>(paths.size());
        try {
            for (int i=0; i < paths.size(); i++) {
                S3PathMetaDTO dto = getDTOFromPath(session, paths.get(i));
                path_dtos.add(dto);
            }
            session.savePersistentAll(path_dtos);
        } finally {
            session.release(path_dtos);
        }
    }

    @Override
    public void deletePaths(List<S3PathMeta> paths)throws StorageException  {
        HopsSession session = connector.obtainSession();
        List<S3PathMetaDTO> path_dtos = new ArrayList<>(paths.size());
        try {
            for (int i=0; i < paths.size(); i++) {
                S3PathMetaDTO dto = getDTOFromPath(session, paths.get(i));
                path_dtos.add(dto);
            }
            session.deletePersistentAll(path_dtos);
        } finally {
            session.release(path_dtos);
        }
    }

    @Override
    public boolean isDirEmpty(String parent, String child, String bucket) throws StorageException {
        String dir_key = parent + '/' + child;
        List<S3PathMeta> dir_children = getPathChildren(dir_key, bucket);
        return dir_children.isEmpty();
    }

    /**
     * Convert a list of S3MetadataDTO's into S3PathMeta
     */
    private List<S3PathMeta> convertAndRelease(HopsSession session, List<S3PathMetaDTO> dtos) throws StorageException {
        List<S3PathMeta> list = new ArrayList(dtos.size());

        for (S3PathMetaDTO dto : dtos) {
            S3PathMeta path = new S3PathMeta();
            path.parent = dto.getParent();
            path.child = dto.getChild();
            path.bucket = dto.getBucket();
            path.isDeleted = NdbBoolean.convert(dto.getIsDeleted());
            path.isDir = NdbBoolean.convert(dto.getIsDir());
            path.blockSize = dto.getBlockSize();
            path.fileLength = dto.getFileLength();
            path.modTime = dto.getModTime();
//            path.tableCreated = dto.getTableCreated();
//            path.tableVersion = dto.getTableVersion();

            list.add(path);
            session.release(dto);
        }

        return list;
    }

    @Override
    public void deleteBucket(String bucketName) {

    }

    @Override
    public List<S3PathMeta> getExpiredFiles(long modTime, String bucket) throws StorageException {
        HopsSession session = connector.obtainSession();
        HopsQueryBuilder qb = session.getQueryBuilder();

        // build the SQL query
        HopsQueryDomainType<S3PathMetaDTO> qdt = qb.createQueryDefinition(S3PathMetaDTO.class);
        HopsPredicate pred1 = qdt.get("modTime").lessEqual(qdt.param("modTimeParam"));
        qdt.where(pred1);

        // Set the query search parameters
        HopsQuery<S3PathMetaDTO> query = session.createQuery(qdt);
        query.setParameter("modTimeParam", modTime);

        return convertAndRelease(session, query.getResultList());
    }

    @Override
    public List<S3PathMeta> getPathChildren(String parent, String bucket) throws StorageException {
        HopsSession session = connector.obtainSession();
        HopsQueryBuilder qb = session.getQueryBuilder();

        // build the SQL query
        HopsQueryDomainType<S3PathMetaDTO> qdt = qb.createQueryDefinition(S3PathMetaDTO.class);
        HopsPredicate pred1 = qdt.get("parent").equal(qdt.param("parent_param"));
        HopsPredicate pred2 = qdt.get("child").isNotNull(); //qdt.param("child_param"));
        HopsPredicate pred3 = qdt.get("bucket").equal(qdt.param("bucket_param"));
        qdt.where(pred1.and(pred2).and(pred3));

        // Set the query search parameters
        HopsQuery<S3PathMetaDTO> query = session.createQuery(qdt);
        query.setParameter("parent_param", parent);
        query.setParameter("bucket_param", bucket);


        return convertAndRelease(session, query.getResultList());
    }
}
