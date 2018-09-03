package com.ignite.utilities;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.cache.configuration.Factory;
import javax.sql.DataSource;

import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.store.jdbc.JdbcType;
import org.apache.ignite.configuration.CacheConfiguration;

public class IgniteAutoConfig {

	private static JdbcType[] jdbcTypes;
	private static Collection<QueryEntity> entities;
	private static List<String> cacheNames;

	private static List<Class<?>> classes;
	private static boolean isHibernate;

	private static Factory<DataSource> dataSource;
	private static final String CLASSNAME = "[IgniteAutoConfig]";

	public static void setDataSource(Factory<DataSource> dataSource) {
		IgniteAutoConfig.dataSource = dataSource;
	}

	public static CacheConfiguration<?, ?>[] generateCacheConfigurations(Class<?> classToConfig) throws Exception {
		if (dataSource == null) {
			throw new Exception (CLASSNAME + " [generateCacheConfig] Datasource has not been specified yet");
		}
		
		int cachesNum = cacheNames.size();
		CacheConfiguration<?, ?>[] cacheConfigs = new CacheConfiguration<?, ?>[cachesNum]; 
		CacheConfigurationFactory cc = new CacheConfigurationFactory();

		for (int i = 0; i < cachesNum; i++) {
			cacheConfigs[i] = cc.generateCacheConfig(classToConfig, cacheNames.get(i), false, false, false);
		}

		return cacheConfigs;
	}

	/**
	 * Change the lookup for the notations from these lib to the javax.persistence
	 * ones.
	 * 
	 * @param isHibernate
	 */
	public static void setIsHibernate(boolean isHibernate) {
		IgniteAutoConfig.isHibernate = isHibernate;
	}

	/**
	 * Add classes to be processed by
	 * 
	 * @param classToAdd
	 * @throws Exception
	 */
	public static void addClass(Class<?> classToAdd) throws Exception {
		if (classes == null) {
			classes = new ArrayList<>();
		}

		classes.add(classToAdd);
	}

	/**
	 * Fill the arrays with the data based on the classes added
	 * 
	 * @throws Exception
	 */
	private static void processNotations() throws Exception {
		jdbcTypes = new JdbcType[classes.size()];
		entities = new ArrayList<>();
		cacheNames = new ArrayList<>();

		if (isHibernate) {
		}

		for (int i = 0; i < jdbcTypes.length; i++) {
			Class<?> igniteClass = classes.get(i);
			ProcessAnnotations pa = new ProcessAnnotations();
			pa.loadData(igniteClass);
			jdbcTypes[i] = pa.getJDBCType();
			entities.add(pa.getQueryEntity());
			cacheNames.add(pa.getCacheName());
		}
	}

	/**
	 * Get the created JDBCType data based on the notations <strong>* It shoud have
	 * classes added and those cannot be added later</strong>
	 * 
	 * @return
	 * @throws Exception
	 */
	public static JdbcType[] getJDBCTypes() throws Exception {
		if (jdbcTypes == null) {
			processNotations();
		}

		return jdbcTypes;
	}

	/**
	 * Get the created query entity based on the notations <strong>* It shoud have
	 * classes added and those cannot be added later</strong>
	 * 
	 * @return
	 * @throws Exception
	 */
	public static Collection<QueryEntity> getQueryEntities() throws Exception {
		if (entities == null) {
			processNotations();
		}

		return entities;
	}
}
