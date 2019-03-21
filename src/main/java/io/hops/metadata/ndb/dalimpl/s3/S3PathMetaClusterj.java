/*
 * Hops Database abstraction layer for storing the hops metadata in MySQL Cluster
 * Copyright (C) 2019  hops.io
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

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


        // Not used yet, since you can just delete the table and re-populate with bucket data easily.
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

        S3PathMetaDTO dto = session.find(S3PathMetaDTO.class, new Object[]{bucket, parent, child});
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
                // Add these back if needed
                //dto.getTableCreated(),
                //dto.getTableVersion()
        );
        session.release(dto);
        return path;

    }

    @Override
    public void putPath(S3PathMeta path) throws StorageException {
        HopsSession session = connector.obtainSession();
        S3PathMetaDTO dto = null;

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
            dto.setBucket(bucket);
            dto.setParent(parent);
            dto.setChild(child);
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
        String dir_key;
        if (parent.endsWith("/")) {
            dir_key = parent + child;
        } else {
            dir_key = parent + "/" + child;
        }
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
            // Add these back if needed
            // path.tableCreated = dto.getTableCreated();
            // path.tableVersion = dto.getTableVersion();

            list.add(path);
            session.release(dto);
        }

        return list;
    }

    @Override
    public void deleteBucket(String bucketName) throws StorageException {
        HopsSession session = connector.obtainSession();
        HopsQueryBuilder qb = session.getQueryBuilder();

        HopsQueryDomainType<S3PathMetaDTO> qdt = qb.createQueryDefinition(S3PathMetaDTO.class);
        HopsPredicate pred1 = qdt.get("bucket").equal(qdt.param("bucketName"));
        qdt.where(pred1);

        // Set the query search parameters
        HopsQuery<S3PathMetaDTO> query = session.createQuery(qdt);
        query.setParameter("bucketName", bucketName);
        query.deletePersistentAll();
    }

    @Override
    public List<S3PathMeta> getExpiredFiles(long modTime, String bucket) throws StorageException {
        HopsSession session = connector.obtainSession();
        HopsQueryBuilder qb = session.getQueryBuilder();

        // build the SQL query
        HopsQueryDomainType<S3PathMetaDTO> qdt = qb.createQueryDefinition(S3PathMetaDTO.class);
        HopsPredicate pred1 = qdt.get("modTime").lessThan(qdt.param("modTimeParam"));
        if (bucket != null) {
            HopsPredicate pred2 = qdt.get("bucket").equal(qdt.param("bucket_param"));
            qdt.where(pred1.and(pred2));
        } else {
            qdt.where(pred1);
        }

        // Set the query search parameters
        HopsQuery<S3PathMetaDTO> query = session.createQuery(qdt);
        if (bucket != null) {
            query.setParameter("modTimeParam", modTime);
            query.setParameter("bucket_param", bucket);
        } else {
            query.setParameter("modTimeParam", modTime);
        }

        return convertAndRelease(session, query.getResultList());
    }

    @Override
    public List<S3PathMeta> getPathChildren(String parent, String bucket) throws StorageException {
        HopsSession session = connector.obtainSession();
        HopsQueryBuilder qb = session.getQueryBuilder();

        // build the SQL query
        HopsQueryDomainType<S3PathMetaDTO> qdt = qb.createQueryDefinition(S3PathMetaDTO.class);
        HopsPredicate pred1 = qdt.get("bucket").equal(qdt.param("bucket_param"));
        HopsPredicate pred2 = qdt.get("parent").equal(qdt.param("parent_param"));
        qdt.where(pred1.and(pred2));

        // Set the query search parameters
        HopsQuery<S3PathMetaDTO> query = session.createQuery(qdt);
        query.setParameter("bucket_param", bucket);
        query.setParameter("parent_param", parent);

        return convertAndRelease(session, query.getResultList());
    }
}
