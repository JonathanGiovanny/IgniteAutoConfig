package com.ignite.utilities;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory;
import org.apache.ignite.cache.store.jdbc.dialect.H2Dialect;
import org.apache.ignite.configuration.CacheConfiguration;

public class CacheConfigurationFactory {

	private int writeBehindFlushFreq = 250;
	
	public CacheConfigurationFactory(int writeBehindFlushFreq) {
		super();
		this.writeBehindFlushFreq = writeBehindFlushFreq;
	}

	public CacheConfigurationFactory() {
		super();
	}
	
	public <K, V> CacheConfiguration<K, V> generateCacheConfig(Class<?> classToConfig, String cacheName, boolean readThrough, boolean writeThrough, boolean writeBehind) throws Exception {
		CacheConfiguration<K, V> cacheConfig = new CacheConfiguration<>();
		cacheConfig.setReadThrough(readThrough);
		cacheConfig.setWriteThrough(writeThrough);
		cacheConfig.setWriteBehindEnabled(writeBehind);
		cacheConfig.setWriteBehindFlushFrequency(writeBehindFlushFreq);

		cacheConfig.setName(cacheName);
		cacheConfig.setAtomicityMode(CacheAtomicityMode.ATOMIC);
		cacheConfig.setBackups(0);

		// DS Factory for the Caches
//		H2DataSourceFactory dsFactory = H2DataSourceFactory.getInstance();

		IgniteAutoConfig.addClass(classToConfig);
		
		CacheJdbcPojoStoreFactory<Object, Object> storeFactory = new CacheJdbcPojoStoreFactory<>();
//		storeFactory.setDataSourceFactory(dsFactory);
		storeFactory.setDialect(new H2Dialect());
		storeFactory.setTypes(IgniteAutoConfig.getJDBCTypes());

		cacheConfig.setCacheStoreFactory(storeFactory);

		cacheConfig.setQueryEntities(IgniteAutoConfig.getQueryEntities());
		return cacheConfig;
	}
	
}
