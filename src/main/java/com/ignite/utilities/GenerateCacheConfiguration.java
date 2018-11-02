package com.ignite.utilities;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.cache.configuration.Factory;
import javax.sql.DataSource;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory;
import org.apache.ignite.cache.store.jdbc.dialect.JdbcDialect;
import org.apache.ignite.configuration.CacheConfiguration;

import com.ignite.utilities.dto.TableDTO;

public class GenerateCacheConfiguration {

	/** Map Key: cacheName, Value: Tables with that cache */
	private static Map<String, List<TableDTO>> cacheTables = new HashMap<>();

	/** List of the caches created */
	private static List<String> cacheNames = new ArrayList<>();

	/** Will define if the configuration is based on the CacheStore object */
	private static boolean isCacheStore;
	
	public static void init(boolean isCacheStore, Map<String, List<TableDTO>> cacheTables, List<String> cacheNames) {
		GenerateCacheConfiguration.isCacheStore = isCacheStore;
		GenerateCacheConfiguration.cacheTables = cacheTables;
		GenerateCacheConfiguration.cacheNames = cacheNames;
	}
	
	public static Map<String, CacheConfiguration<?, ?>> generateCacheConfiguration(Factory<DataSource> dataSource, JdbcDialect dialect) {
		Map<String, CacheConfiguration<?, ?>> cacheConfigs = new HashMap<>();

		try {
			for (String cacheName : cacheNames) {
				List<TableDTO> tablesPerCache = cacheTables.get(cacheName);

				// Generate a cacheConfiguration per cacheName
				CacheConfiguration<Long, ?> cacheConfig = new CacheConfiguration<>();
				cacheConfig.setReadThrough(true);
				cacheConfig.setWriteThrough(true);
				cacheConfig.setWriteBehindEnabled(true);
				cacheConfig.setWriteBehindFlushFrequency(250);

				cacheConfig.setName(cacheName);
				cacheConfig.setAtomicityMode(CacheAtomicityMode.ATOMIC);
				cacheConfig.setBackups(0);

				if (isCacheStore) {
					generateCacheStore(cacheConfig, tablesPerCache);
				} else {
					generateJDBCStore(cacheConfig, dataSource, dialect, tablesPerCache);
				}

				// Add the cacheConfiguration created to a list
				cacheConfigs.put(cacheName, cacheConfig);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return cacheConfigs;
	}

	/**
	 * 
	 * @param cacheConfig
	 */
	private static void generateCacheStore(CacheConfiguration<Long, ?> cacheConfig, List<TableDTO> tablesPerCache) {
	}

	/**
	 * 
	 * @param cacheConfig
	 * @param dataSource
	 * @param dialect
	 * @throws Exception 
	 */
	private static void generateJDBCStore(CacheConfiguration<?, ?> cacheConfig, Factory<DataSource> dataSource, JdbcDialect dialect, List<TableDTO> tablesPerCache) throws Exception {
		Collection<QueryEntity> queryEntities = new ArrayList<>();

		// One store factory per cacheConfig with the data of each Table
		CacheJdbcPojoStoreFactory<Object, Object> storeFactory = new CacheJdbcPojoStoreFactory<>();
		storeFactory.setDataSourceFactory(dataSource);
		storeFactory.setDialect(dialect);

		for (TableDTO tableData : tablesPerCache) {
			// Generates the JdbcType data and QueryEntity for the table
			GenerateMapping gm = new GenerateMapping();
			gm.createTableSchema(tableData.getCacheName(), tableData);

			storeFactory.setTypes(gm.getJdbcType());

			queryEntities.add(gm.getQueryEntity());
		}

		cacheConfig.setCacheStoreFactory(storeFactory);
		cacheConfig.setQueryEntities(queryEntities);
	}
}
