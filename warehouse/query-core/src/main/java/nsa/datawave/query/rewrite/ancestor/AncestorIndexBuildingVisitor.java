package nsa.datawave.query.rewrite.ancestor;

import com.google.common.base.Predicate;

import java.io.IOException;
import java.util.*;

import com.google.common.collect.Maps;
import nsa.datawave.core.iterators.filesystem.FileSystemCache;
import nsa.datawave.core.iterators.querylock.QueryLock;
import nsa.datawave.query.rewrite.Constants;
import nsa.datawave.query.rewrite.function.Equality;
import nsa.datawave.query.rewrite.iterator.SourceFactory;
import nsa.datawave.query.rewrite.jexl.JexlASTHelper;
import nsa.datawave.query.rewrite.jexl.functions.FieldIndexAggregator;
import nsa.datawave.query.rewrite.jexl.visitors.IteratorBuildingVisitor;
import nsa.datawave.query.rewrite.predicate.TimeFilter;
import nsa.datawave.query.rewrite.tld.TLD;
import nsa.datawave.query.util.IteratorToSortedKeyValueIterator;
import nsa.datawave.query.util.TypeMetadata;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * Custom IndexBuildingVisitor that will expand (simulate) fi indexes into the entire branch of the document
 */
public class AncestorIndexBuildingVisitor extends IteratorBuildingVisitor {
    private static final Logger log = Logger.getLogger(AncestorIndexBuildingVisitor.class);
    
    private Map<String,Collection<String>> familyTreeMap;
    private Map<String,Long> timestampMap;
    private Equality equality;
    
    public AncestorIndexBuildingVisitor(SourceFactory<Key,Value> sourceFactory,
                    IteratorEnvironment env,
                    TimeFilter timeFilter,
                    TypeMetadata typeMetadata,
                    Set<String> indexOnlyFields,
                    // EventDataQueryFilter attrFilter,
                    Predicate<Key> datatypeFilter, FieldIndexAggregator fiAggregator, FileSystemCache fileSystemCache, QueryLock queryLock,
                    List<String> hdfsCacheDirURIAlternatives, String queryId, String hdfsCacheSubDirPrefix, String hdfsFileCompressionCodec,
                    int hdfsCacheBufferSize, long hdfsCacheScanPersistThreshold, long hdfsCacheScanTimeout, int maxRangeSplit, int maxOpenFiles,
                    int maxIvaratorSources, Collection<String> includes, Collection<String> excludes, Set<String> termFrequencyFields,
                    boolean isQueryFullySatisfied, boolean sortedUIDs, Equality equality) {
        super(sourceFactory, env, timeFilter, typeMetadata, indexOnlyFields, datatypeFilter, fiAggregator, fileSystemCache, queryLock,
                        hdfsCacheDirURIAlternatives, queryId, hdfsCacheSubDirPrefix, hdfsFileCompressionCodec, hdfsCacheBufferSize,
                        hdfsCacheScanPersistThreshold, hdfsCacheScanTimeout, maxRangeSplit, maxOpenFiles, maxIvaratorSources, includes, excludes,
                        termFrequencyFields, isQueryFullySatisfied, sortedUIDs);
        setIteratorBuilder(AncestorIndexIteratorBuilder.class);
        this.equality = equality;
        familyTreeMap = new HashMap<>();
        timestampMap = new HashMap<>();
    }
    
    @Override
    protected SortedKeyValueIterator<Key,Value> getSourceIterator(final ASTEQNode node, boolean negation) {
        
        SortedKeyValueIterator<Key,Value> kvIter = null;
        try {
            if (limitLookup && !negation) {
                final String identifier = JexlASTHelper.getIdentifier(node);
                if (!disableFiEval && indexOnlyFields.contains(identifier)) {
                    kvIter = source.deepCopy(env);
                    // restrict the ranges across this document and go look these up, kvIter now will return valid fi ranges
                    seekIndexOnlyDocument(kvIter, node);
                    kvIter = new IteratorToSortedKeyValueIterator(expandChildren(node, kvIter).iterator());
                } else {
                    kvIter = new IteratorToSortedKeyValueIterator(getNodeEntry(node).iterator());
                }
            } else {
                kvIter = source.deepCopy(env);
                seekIndexOnlyDocument(kvIter, node);
            }
            
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        return kvIter;
    }
    
    @Override
    protected void seekIndexOnlyDocument(SortedKeyValueIterator<Key,Value> kvIter, ASTEQNode node) throws IOException {
        if (null != rangeLimiter && limitLookup) {
            Key startKey = getKey(node);
            Key endKey = getEndKey(node);
            
            kvIter.seek(new Range(startKey, true, endKey, true), Collections.<ByteSequence> emptyList(), false);
        }
    }
    
    protected Key getEndKey(JexlNode node) {
        Key endKey = rangeLimiter.getEndKey();
        String identifier = JexlASTHelper.getIdentifier(node);
        Object objValue = JexlASTHelper.getLiteralValue(node);
        String value = null == objValue ? "null" : objValue.toString();
        
        StringBuilder builder = new StringBuilder("fi");
        builder.append(NULL_DELIMETER).append(identifier);
        
        Text cf = new Text(builder.toString());
        
        builder = new StringBuilder(value);
        
        builder.append(NULL_DELIMETER).append(endKey.getColumnFamily());
        Text cq = new Text(builder.toString());
        
        return new Key(endKey.getRow(), cf, cq, endKey.getTimestamp());
    }
    
    private Collection<String> getMembers() {
        Range wholeDocRange = getWholeDocRange(rangeLimiter);
        final String tld = getTLDId(wholeDocRange.getStartKey());
        final String dataType = getDataType(wholeDocRange.getStartKey());
        Collection<String> members = familyTreeMap.get(tld);
        
        // use the cached tree if available
        if (members == null) {
            SortedKeyValueIterator<Key,Value> kvIter = source.deepCopy(env);
            members = getMembers(wholeDocRange.getStartKey().getRow().toString(), tld, dataType, kvIter);
            
            // set the members for later use
            familyTreeMap.put(tld, members);
        }
        
        return members;
    }
    
    protected Collection<Map.Entry<Key,Value>> expandChildren(ASTEQNode node, SortedKeyValueIterator<Key,Value> hits) throws IOException {
        final List<Map.Entry<Key,Value>> keys = new ArrayList<>();
        
        final Range wholeDocRange = getWholeDocRange(rangeLimiter);
        final String dataType = getDataType(wholeDocRange.getStartKey());
        final Collection<String> members = getMembers();
        
        final Text row = rangeLimiter.getStartKey().getRow();
        while (hits.hasTop()) {
            Key top = hits.getTopKey();
            String cq = top.getColumnQualifier().toString();
            int uidIndex = cq.lastIndexOf(Constants.NULL_BYTE_STRING);
            String uidHit = cq.substring(uidIndex + 1);
            for (String child : members) {
                if (equality.partOf(new Key("", child), new Key("", uidHit))) {
                    Long timestamp = timestampMap.get(child);
                    if (timestamp == null) {
                        timestamp = rangeLimiter.getStartKey().getTimestamp();
                    }
                    keys.add(Maps.immutableEntry(getKey(node, row, dataType, child, timestamp), Constants.NULL_VALUE));
                }
            }
            hits.next();
        }
        
        return keys;
    }
    
    /**
     * Expand node entry from the single fi that is generated by this node, and instead generate keys for the entire document branch
     * 
     * @param node
     * @return
     */
    @Override
    protected Collection<Map.Entry<Key,Value>> getNodeEntry(ASTEQNode node) {
        final List<Map.Entry<Key,Value>> keys = new ArrayList<>();
        Range wholeDocRange = getWholeDocRange(rangeLimiter);
        final String tld = getTLDId(wholeDocRange.getStartKey());
        final String dataType = getDataType(wholeDocRange.getStartKey());
        Collection<String> members = familyTreeMap.get(tld);
        
        // use the cached tree if available
        if (members == null) {
            SortedKeyValueIterator<Key,Value> kvIter = source.deepCopy(env);
            members = getMembers(wholeDocRange.getStartKey().getRow().toString(), tld, dataType, kvIter);
            
            // set the members for later use
            familyTreeMap.put(tld, members);
        }
        
        for (String uid : members) {
            // only generate index keys beyond the current uid in the tree
            Key rangeCheckKey = new Key(rangeLimiter.getStartKey().getRow().toString(), dataType + Constants.NULL_BYTE_STRING + uid);
            if (!rangeLimiter.beforeStartKey(rangeCheckKey) && !rangeLimiter.afterEndKey(rangeCheckKey)) {
                Long timestamp = timestampMap.get(uid);
                if (timestamp == null) {
                    timestamp = rangeLimiter.getStartKey().getTimestamp();
                }
                keys.add(Maps.immutableEntry(getKey(node, rangeLimiter.getStartKey().getRow(), dataType, uid, timestamp), Constants.NULL_VALUE));
            }
        }
        
        return keys;
    }
    
    /**
     * Get all uids for a given tldUid and dataType and row from the iterator, seeking between keys
     * 
     * @param row
     * @param tldUid
     * @param dataType
     * @param iterator
     * @return
     */
    private List<String> getMembers(String row, String tldUid, String dataType, SortedKeyValueIterator<Key,Value> iterator) {
        final List<String> members = new ArrayList<>();
        Key startKey = new Key(row, dataType + Constants.NULL_BYTE_STRING + tldUid);
        Key endKey = new Key(row, dataType + Constants.NULL_BYTE_STRING + tldUid + Constants.MAX_UNICODE_STRING);
        
        // inclusive to catch the first uid
        Range range = new Range(startKey, true, endKey, false);
        try {
            iterator.seek(range, Collections.<ByteSequence> emptyList(), false);
            
            while (iterator.hasTop()) {
                Key nextKey = iterator.getTopKey();
                String keyTld = getTLDId(nextKey);
                if (keyTld.equals(tldUid)) {
                    String uid = getUid(nextKey);
                    members.add(uid);
                    timestampMap.put(uid, nextKey.getTimestamp());
                } else {
                    break;
                }
                
                // seek to the next child by shifting the startKey
                startKey = new Key(row, nextKey.getColumnFamily().toString() + Constants.NULL_BYTE_STRING);
                iterator.seek(new Range(startKey, true, endKey, true), Collections.<ByteSequence> emptyList(), false);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        return members;
    }
    
    /**
     * Extract the data type from an fi key
     * 
     * @param key
     * @return
     */
    private String getDataType(Key key) {
        String cf = key.getColumnFamily().toString();
        final int splitIndex = cf.indexOf('\0');
        
        if (splitIndex > -1) {
            return cf.substring(0, splitIndex);
        }
        
        return null;
    }
    
    /**
     * Extract the uid from an event key, format shardId dataType\0UID FieldName\0FieldValue NULL
     * 
     * @param key
     * @return
     */
    private String getUid(Key key) {
        Text startColfam = key.getColumnFamily();
        if (startColfam.find(Constants.NULL) != -1) {
            // have a start key with a document uid, add to the end of the cf to ensure we go to the next doc
            // parse out the uid
            String cf = startColfam.toString();
            int index = cf.indexOf('\0');
            if (index >= 0) {
                String uid = cf.substring(index + 1);
                
                return uid;
            }
        }
        
        return null;
    }
    
    /**
     * Extract the TLD uid from an event key
     * 
     * @param key
     * @return
     */
    private String getTLDId(Key key) {
        String uid = getUid(key);
        
        // if the uid is not empty
        if (!uid.isEmpty()) {
            uid = TLD.parseRootPointerFromId(uid);
        }
        
        return uid;
    }
    
    /**
     * Expand a range to include an entire document if it includes a specific event
     * 
     * @param r
     * @return
     */
    protected Range getWholeDocRange(final Range r) {
        Range result = r;
        
        Key start = r.getStartKey();
        Key end = r.getEndKey();
        String endCf = (end == null || end.getColumnFamily() == null ? "" : end.getColumnFamily().toString());
        String startCf = (start == null || start.getColumnFamily() == null ? "" : start.getColumnFamily().toString());
        
        // if the end key inclusively includes a datatype\0UID or has datatype\0UID\0, then move the key past the children
        if (endCf.length() > 0 && (r.isEndKeyInclusive() || endCf.charAt(endCf.length() - 1) == '\0')) {
            String row = end.getRow().toString().intern();
            if (endCf.charAt(endCf.length() - 1) == '\0') {
                endCf = endCf.substring(0, endCf.length() - 1);
            }
            Key postDoc = new Key(row, endCf + Character.MAX_CODE_POINT);
            result = new Range(r.getStartKey(), r.isStartKeyInclusive(), postDoc, false);
        }
        
        // if the start key is not inclusive, and we have a datatype\0UID, then move the start past the children thereof
        if (!r.isStartKeyInclusive() && startCf.length() > 0) {
            // we need to bump append 0xff to that byte array because we want to skip the children
            String row = start.getRow().toString().intern();
            
            Key postDoc = new Key(row, startCf + Character.MAX_CODE_POINT);
            // if this puts us past the end of teh range, then adjust appropriately
            if (result.contains(postDoc)) {
                result = new Range(postDoc, false, result.getEndKey(), result.isEndKeyInclusive());
            } else {
                result = new Range(result.getEndKey(), false, result.getEndKey().followingKey(PartialKey.ROW_COLFAM), false);
            }
        }
        
        return result;
    }
    
    /**
     * Generate a new fi key from the current node with the specific row/dataType/uid/timestamp
     * 
     * @param node
     * @param row
     * @param dataType
     * @param uid
     * @param timestamp
     * @return
     */
    private Key getKey(JexlNode node, Text row, String dataType, String uid, long timestamp) {
        String fieldName = JexlASTHelper.getIdentifier(node);
        Object objValue = JexlASTHelper.getLiteralValue(node);
        String fieldValuie = null == objValue ? "null" : objValue.toString();
        
        StringBuilder builder = new StringBuilder("fi");
        builder.append(NULL_DELIMETER).append(fieldName);
        Text cf = new Text(builder.toString());
        
        builder = new StringBuilder(fieldValuie);
        builder.append(NULL_DELIMETER).append(dataType).append(NULL_DELIMETER).append(uid);
        Text cq = new Text(builder.toString());
        
        return new Key(row, cf, cq, timestamp);
    }
}
