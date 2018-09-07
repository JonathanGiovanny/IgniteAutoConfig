package com.ignite.utilities;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.cache.configuration.Factory;
import javax.sql.DataSource;

import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.store.jdbc.JdbcType;
import org.apache.ignite.configuration.CacheConfiguration;

import com.ignite.utilities.dto.TableDTO;

public class IgniteAutoConfig {

	/** Map Key: cacheName, Value: Tables with that cache */
	// private static Map<String, List<TableDTO>> tables;

	private static JdbcType[] jdbcTypes;
	private static Collection<QueryEntity> entities;
	private static List<String> cacheNames;

	private static List<Class<?>> classes;
	private static List<TableDTO> tables = new ArrayList<>();
	private static boolean isHibernate;

	private static final String CLASSNAME = "[IgniteAutoConfig]";

	// ---------------------------- Methods to generate the JDBCType and the Query Entities -------------------------//

	// ------------------------------------ Methods to generate the cacheConfiguration ------------------------------//

	// --------------------------------------------- Configuration methods ------------------------------------------//
	/**
	 * Generates the cacheConfiguration instance for each class
	 * 
	 * @return
	 * @throws Exception
	 */
	public static CacheConfiguration<?, ?>[] generateCacheConfigurations(Factory<DataSource> dataSource) throws Exception {
		if (dataSource == null) {
			throw new Exception(CLASSNAME + " [generateCacheConfig] Datasource has not been specified yet");
		}

		int cachesNum = cacheNames.size();
		CacheConfiguration<?, ?>[] cacheConfigs = new CacheConfiguration<?, ?>[cachesNum];
		CacheConfigurationFactory cc = new CacheConfigurationFactory(dataSource);

		for (int i = 0; i < cachesNum; i++) {
			cacheConfigs[i] = cc.generateCacheConfig(classes.get(i), cacheNames.get(i), false, false, false);
		}

		return cacheConfigs;
	}

	/**
	 * Change the lookup for the notations from these lib to the javax.persistence ones.
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

		ProcessAnnotationsDTO pa = new ProcessAnnotationsDTO();
		tables.add(pa.loadClassData(classToAdd));

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
	 * Get the created JDBCType data based on the notations <strong>* It shoud have classes added and those cannot be added later</strong>
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
	 * Get the created query entity based on the notations <strong>* It shoud have classes added and those cannot be added later</strong>
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

	/**
	 * Get the list of cacheNames mapped
	 * 
	 * @return
	 */
	public static List<String> getCacheNames() {
		return cacheNames;
	}
}
