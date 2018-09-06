package com.ignite.utilities;

import javax.cache.configuration.Factory;
import javax.sql.DataSource;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory;
import org.apache.ignite.cache.store.jdbc.dialect.H2Dialect;
import org.apache.ignite.configuration.CacheConfiguration;

public class CacheConfigurationFactory {

	private int writeBehindFlushFreq = 250;
	// DS Factory for the Caches
	private Factory<DataSource> dataSource;
	
	public CacheConfigurationFactory(int writeBehindFlushFreq) {
		super();
		this.writeBehindFlushFreq = writeBehindFlushFreq;
	}

	public CacheConfigurationFactory(Factory<DataSource> dataSource) {
		super();
		this.dataSource = dataSource;
	}
	
	public CacheConfiguration<?, ?> generateCacheConfig(Class<?> classToConfig, String cacheName, boolean readThrough, boolean writeThrough, boolean writeBehind) throws Exception {
		CacheConfiguration<?, ?> cacheConfig = new CacheConfiguration<>();
		cacheConfig.setReadThrough(readThrough);
		cacheConfig.setWriteThrough(writeThrough);
		cacheConfig.setWriteBehindEnabled(writeBehind);
		cacheConfig.setWriteBehindFlushFrequency(writeBehindFlushFreq);

		cacheConfig.setName(cacheName);
		cacheConfig.setAtomicityMode(CacheAtomicityMode.ATOMIC);
		cacheConfig.setBackups(0);

		CacheJdbcPojoStoreFactory<Object, Object> storeFactory = new CacheJdbcPojoStoreFactory<>();
		storeFactory.setDataSourceFactory(dataSource);
		storeFactory.setDialect(new H2Dialect());
		storeFactory.setTypes(IgniteAutoConfig.getJDBCTypes());

		cacheConfig.setCacheStoreFactory(storeFactory);

		cacheConfig.setQueryEntities(IgniteAutoConfig.getQueryEntities());
		return cacheConfig;
	}
	
}
