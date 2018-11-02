package com.ignite.utilities;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.stream.Collectors;

import javax.cache.Cache.Entry;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;

import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.resources.CacheStoreSessionResource;

import com.ignite.utilities.dto.ColumnDTO;
import com.ignite.utilities.dto.TableDTO;

@SuppressWarnings("rawtypes")
public class GenerateCacheStore extends CacheStoreAdapter<Long, Object> {

	/** Store session. */
	@CacheStoreSessionResource
	private CacheStoreSession ses;
	private TableDTO table;
	private Exception e;
	private static final String SET_QM = " = ?";

	@Override
	public Object load(Long key) throws CacheLoaderException {
		return null;
	}

	@Override
	public void write(Entry entry) throws CacheWriterException {
		// Get the key and value for every insert call from the invocation class
		// IgniteWriteThroughCache
		Object key = entry.getKey();
		Object value = entry.getValue();

		PreparedStatement ps = null;

		try {
			Connection conn = ses.attachment();
			// Generate and set the insert query to be executed
			ps = conn.prepareStatement(buildInsertQuery());
			boolean isSuccesful = tryPS(key, ps);

			// If the insert fail, run the update because the record already exists
			if (!isSuccesful) {
				ps = conn.prepareStatement(buildUpdateQuery());
				isSuccesful = tryPS(key, ps);
			}

			// If everything fails, throws the captured exception
			if (!isSuccesful) {
				throw e;
			}

		} catch (Exception ex) {
			throw new CacheLoaderException("Failed to put object [key=" + key + ", val=" + value + ']', ex);

		} finally {
			closeConnection(ps);
		}
	}

	@Override
	public void delete(Object key) throws CacheWriterException {
	}

	/**
	 * Insert or Update into the DB and try (could fail due to existing record or
	 * Constraint)
	 * 
	 * @param key
	 * @param ps
	 * @return
	 */
	private boolean tryPS(Object key, PreparedStatement ps) {
		try {
			int i = 0;
			while (i < table.getColumns().size()) {
				ps.setObject(i, table.getClass().getDeclaredFields()[i + 1]);
				i++;
			}
			ps.setObject(i, key);
			return true;

		} catch (Exception e) {
			this.e = e;
			return false;
		}
	}

	/**
	 * Build Query to insert with the Question Marks to set the data via ps
	 * 
	 * @return
	 */
	private String buildInsertQuery() {
		StringBuilder sb = new StringBuilder();

		if (table != null) {
			sb.append("INSERT INTO " + table.getTableType() + " (");

			StringBuilder sbQM = new StringBuilder();

			List<ColumnDTO> columns = table.getColumns();
			for (ColumnDTO column : columns) {
				sb.append(column.getColumnName() + ", ");
				sbQM.append(" ?, ");
			}
			sbQM.replace(sb.length() - 2, sb.length() - 1, "");
			sb.replace(sb.length() - 2, sb.length(), "");

			String keyColumn = table.getColumns().stream().filter(k -> k.isKey()).collect(Collectors.toList()).get(0)
					.getColumnName();
			sb.append(") VALUES (" + sbQM.toString() + ") WHERE " + keyColumn + SET_QM);
		}

		return sb.toString();
	}

	/**
	 * Build Query to update with the Question Marks to set the data via ps
	 * 
	 * @return
	 */
	private String buildUpdateQuery() {
		StringBuilder sb = new StringBuilder();

		if (table != null) {
			sb.append("UPDATE " + table.getTableType() + " SET");

			List<ColumnDTO> columns = table.getColumns();
			for (ColumnDTO column : columns) {
				sb.append(column.getColumnName() + " = ?, ");
			}
			sb.replace(sb.length() - 2, sb.length() - 1, "");

			String keyColumn = table.getColumns().stream().filter(k -> k.isKey()).collect(Collectors.toList()).get(0)
					.getColumnName();
			sb.append("WHERE " + keyColumn + SET_QM);
		}

		return sb.toString();
	}

	/**
	 * Close connection
	 * 
	 * @param ps
	 */
	private void closeConnection(PreparedStatement ps) {
		try {
			if (ps != null) {
				ps.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void setTable(TableDTO table) {
		this.table = table;
	}
}
