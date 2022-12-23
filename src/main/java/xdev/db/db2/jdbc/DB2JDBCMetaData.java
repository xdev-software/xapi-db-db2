package xdev.db.db2.jdbc;

/*-
 * #%L
 * DB2
 * %%
 * Copyright (C) 2003 - 2022 XDEV Software
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Lesser Public License for more details.
 * 
 * You should have received a copy of the GNU General Lesser Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/lgpl-3.0.html>.
 * #L%
 */


import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.xdev.jadoth.sqlengine.interfaces.ConnectionProvider;

import xdev.db.ColumnMetaData;
import xdev.db.DBException;
import xdev.db.DataType;
import xdev.db.Index;
import xdev.db.Index.IndexType;
import xdev.db.Result;
import xdev.db.StoredProcedure;
import xdev.db.StoredProcedure.Param;
import xdev.db.StoredProcedure.ParamType;
import xdev.db.StoredProcedure.ReturnTypeFlavor;
import xdev.db.jdbc.JDBCColumnsMetaData;
import xdev.db.jdbc.JDBCConnection;
import xdev.db.jdbc.JDBCDataSource;
import xdev.db.jdbc.JDBCMetaData;
import xdev.db.sql.Functions;
import xdev.db.sql.SELECT;
import xdev.db.sql.Table;
import xdev.util.ProgressMonitor;


public class DB2JDBCMetaData extends JDBCMetaData
{
	private static final long	serialVersionUID	= 2862594319338582561L;
	
	// constant strings
	private static final String	PARAMETER_TYPE		= "PARAMETER_TYPE";
	private static final String	SQLJ				= "SQLJ";
	private static final String	SYS					= "SYS";
	private static final String	TYPE				= "_TYPE";
	private static final String	REMARKS				= "REMARKS";
	private static final String	SCHEM				= "_SCHEM";
	private static final String	NAME				= "_NAME";
	private static final String	DATA_TYPE			= "DATA_TYPE";
	private static final String	COLUMN_TYPE			= "COLUMN_TYPE";
	private static final String	COLUMN_NAME			= "COLUMN_NAME";
	private static final String	PROCEDURE			= "PROCEDURE";
	private static final String	FUNCTION			= "FUNCTION";
	
	
	public DB2JDBCMetaData(DB2JDBCDataSource dataSource) throws DBException
	{
		super(dataSource);
	}
	
	
	@Override
	protected String getCatalog(JDBCDataSource dataSource)
	{
		return null;
	}
	
	
	@Override
	public TableInfo[] getTableInfos(ProgressMonitor monitor, EnumSet<TableType> types)
			throws DBException
	{
		monitor.beginTask("",ProgressMonitor.UNKNOWN);
		
		List<TableInfo> list = new ArrayList<TableInfo>();
		
		JDBCConnection jdbcConnection = (JDBCConnection)dataSource.openConnection();
		
		try
		{
			String schema = getSchema(dataSource);
			String sql;
			String tableTypeStatement = getTableTypeStatement(types);
			
			if(schema != null && schema.length() > 0)
			{
				sql = "Select tabschema, tabname, type from syscat.tables where tabschema = '"
						+ schema + "' and " + tableTypeStatement + " for read only";
			}
			else
			{
				sql = "Select tabschema, tabname, type from syscat.tables where tabschema not like 'SYS%' "
						+ "and tabschema not like 'DB2QP' and "
						+ tableTypeStatement
						+ " for read only";
			}
			
			Result rs = jdbcConnection.query(sql);
			while(rs.next() && !monitor.isCanceled())
			{
				String type = rs.getString("type");
				TableType tableType = type.equalsIgnoreCase("T") ? TableType.TABLE : TableType.VIEW;
				if(types.contains(tableType))
				{
					list.add(new TableInfo(tableType,rs.getString("tabschema"),rs
							.getString("tabname")));
				}
			}
			rs.close();
			
		}
		finally
		{
			jdbcConnection.close();
		}
		
		monitor.done();
		
		TableInfo[] tables = list.toArray(new TableInfo[list.size()]);
		Arrays.sort(tables);
		return tables;
	}
	
	
	@Override
	protected TableMetaData getTableMetaData(JDBCConnection jdbcConnection, DatabaseMetaData meta,
			int flags, TableInfo table) throws DBException, SQLException
	{
		String catalog = getCatalog(dataSource);
		String schema = getSchema(dataSource);
		
		String tableName = table.getName();
		Table tableIdentity = new Table(tableName);
		
		Map<String, Object> defaultValues = new HashMap<String, Object>();
		ResultSet rs = meta.getColumns(catalog,schema,tableName,null);
		while(rs.next())
		{
			String columnName = rs.getString(COLUMN_NAME);
			Object defaultValue = rs.getObject("COLUMN_DEF");
			defaultValues.put(columnName,defaultValue);
		}
		rs.close();
		
		SELECT select = new SELECT().FROM(tableIdentity).WHERE("1 = 0");
		Result result = jdbcConnection.query(select);
		int cc = result.getColumnCount();
		ColumnMetaData[] columns = new ColumnMetaData[cc];
		for(int i = 0; i < cc; i++)
		{
			ColumnMetaData column = result.getMetadata(i);
			
			Object defaultValue = column.getDefaultValue();
			if(defaultValue == null && defaultValues.containsKey(column.getName()))
			{
				defaultValue = defaultValues.get(column.getName());
			}
			defaultValue = checkDefaultValue(defaultValue,column);
			
			columns[i] = new ColumnMetaData(tableName,column.getName(),column.getCaption(),
					column.getType(),column.getLength(),column.getScale(),defaultValue,
					column.isNullable(),column.isAutoIncrement());
		}
		result.close();
		
		Map<IndexInfo, Set<String>> indexMap = new Hashtable<IndexInfo, Set<String>>();
		int count = UNKNOWN_ROW_COUNT;
		
		if(table.getType() == TableType.TABLE)
		{
			Set<String> primaryKeyColumns = new HashSet<String>();
			rs = meta.getPrimaryKeys(catalog,schema,tableName);
			while(rs.next())
			{
				primaryKeyColumns.add(rs.getString(COLUMN_NAME));
			}
			rs.close();
			
			if((flags & INDICES) != 0)
			{
				if(primaryKeyColumns.size() > 0)
				{
					indexMap.put(new IndexInfo("PRIMARY_KEY",IndexType.PRIMARY_KEY),
							primaryKeyColumns);
				}
				
				rs = meta.getIndexInfo(catalog,schema,tableName,false,true);
				while(rs.next())
				{
					String indexName = rs.getString("INDEX_NAME");
					String columnName = rs.getString(COLUMN_NAME);
					if(indexName != null && columnName != null
							&& !primaryKeyColumns.contains(columnName))
					{
						boolean unique = !rs.getBoolean("NON_UNIQUE");
						IndexInfo info = new IndexInfo(indexName,unique ? IndexType.UNIQUE
								: IndexType.NORMAL);
						Set<String> columnNames = indexMap.get(info);
						if(columnNames == null)
						{
							columnNames = new HashSet<String>();
							indexMap.put(info,columnNames);
						}
						columnNames.add(columnName);
					}
				}
				rs.close();
			}
			
			if((flags & ROW_COUNT) != 0)
			{
				try
				{
					result = jdbcConnection.query(new SELECT().columns(Functions.COUNT()).FROM(
							tableIdentity));
					if(result.next())
					{
						count = result.getInt(0);
					}
					result.close();
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
			}
		}
		
		Index[] indices = new Index[indexMap.size()];
		int i = 0;
		for(IndexInfo indexInfo : indexMap.keySet())
		{
			Set<String> columnList = indexMap.get(indexInfo);
			String[] indexColumns = columnList.toArray(new String[columnList.size()]);
			indices[i++] = new Index(indexInfo.name,indexInfo.type,indexColumns);
		}
		
		TableMetaData tableMeta = new TableMetaData(table,columns,indices,count);
		
		for(ColumnMetaData column : tableMeta.getColumns())
		{
			Object def = column.getDefaultValue();
			if(def instanceof String)
			{
				String str = (String)def;
				int length = str.length();
				if(length >= 2)
				{
					if(str.charAt(0) == '\'' && str.charAt(length - 1) == '\'')
					{
						column.setDefaultValue(str.substring(1,length - 1));
					}
				}
			}
		}
		
		return tableMeta;
	}
	
	
	@Override
	public StoredProcedure[] getStoredProcedures(ProgressMonitor monitor) throws DBException
	{
		monitor.beginTask("",ProgressMonitor.UNKNOWN);
		
		List<StoredProcedure> list = new ArrayList<StoredProcedure>();
		
		try
		{
			ConnectionProvider<?> connectionProvider = dataSource.getConnectionProvider();
			Connection connection = connectionProvider.getConnection();
			
			try
			{
				DatabaseMetaData meta = connection.getMetaData();
				
				String catalog = getCatalog(dataSource);
				String schema = getSchema(dataSource);
				
				// Stored Procedures
				ResultSet procedures = meta.getProcedures(catalog,schema,null);
				ResultSet procedureColumns = meta.getProcedureColumns(catalog,schema,null,null);
				
				Map<String, List<JDBCColumnsMetaData>> procedureColumnsMap = columnsResultSetToMap(
						procedureColumns,PROCEDURE);
				addStoredProcedures(list,procedures,procedureColumnsMap,PROCEDURE);
				
				// Functions
				ResultSet functions = meta.getFunctions(catalog,schema,null);
				ResultSet functionColumns = meta.getFunctionColumns(catalog,schema,null,null);
				
				Map<String, List<JDBCColumnsMetaData>> functionColumnMap = columnsResultSetToMap(
						functionColumns,FUNCTION);
				addStoredProcedures(list,functions,functionColumnMap,FUNCTION);
				
			}
			finally
			{
				connection.close();
			}
		}
		catch(SQLException e)
		{
			throw new DBException(dataSource,e);
		}
		
		monitor.done();
		
		return list.toArray(new StoredProcedure[list.size()]);
		
	}
	
	
	private Map<String, List<JDBCColumnsMetaData>> columnsResultSetToMap(ResultSet resultSet,
			String prefix) throws SQLException
	{
		Map<String, List<JDBCColumnsMetaData>> resultMap = new HashMap<String, List<JDBCColumnsMetaData>>();
		while(resultSet.next())
		{
			
			String name = resultSet.getString(prefix.concat(NAME));
			
			// skip system functions
			String schema = resultSet.getString(prefix.concat(SCHEM));
			if(schema != null && (schema.startsWith(SYS) || schema.startsWith(SQLJ)))
			{
				continue;
			}
			
			int columnType = 0;
			String columnName = "";
			if(prefix.equals(FUNCTION))
			{
				// XXX "PARAMETER_TYPE" works but it should be "COLUMN_TYPE"
				// according to the javadoc
				columnType = resultSet.getInt(PARAMETER_TYPE);
				
				// XXX better get columnName using name of the column, but don't
				// know it
				columnName = resultSet.getString(4);
				
			}
			else
			{
				columnName = resultSet.getString(COLUMN_NAME);
				columnType = resultSet.getInt(COLUMN_TYPE);
			}
			
			DataType dataType = DataType.get(resultSet.getInt(DATA_TYPE));
			
			if(resultMap.containsKey(name))
			{
				List<JDBCColumnsMetaData> metaDataList = resultMap.get(name);
				JDBCColumnsMetaData metaData = new JDBCColumnsMetaData(dataType,columnType,
						columnName);
				
				metaDataList.add(metaData);
				
			}
			else
			{
				List<JDBCColumnsMetaData> metaDataList = new ArrayList<JDBCColumnsMetaData>();
				JDBCColumnsMetaData metaData = new JDBCColumnsMetaData(dataType,columnType,
						columnName);
				metaDataList.add(metaData);
				
				resultMap.put(name,metaDataList);
			}
			
		}
		resultSet.close();
		return resultMap;
	}
	
	
	private void addStoredProcedures(List<StoredProcedure> list, ResultSet resultSet,
			Map<String, List<JDBCColumnsMetaData>> resultMap, String prefix) throws SQLException
	{
		while(resultSet.next())
		{
			
			// skip system functions
			String schema = resultSet.getString(prefix.concat(SCHEM));
			if(schema != null && (schema.startsWith(SYS) || schema.startsWith(SQLJ)))
			{
				continue;
			}
			
			String name = resultSet.getString(prefix.concat(NAME));
			String description = resultSet.getString(REMARKS);
			ReturnTypeFlavor returnTypeFlavor = null;
			DataType returnType = null;
			
			int type = 0;
			if(prefix.equals(FUNCTION))
			{
				// XXX jdbc driver is not able to return function type
				// procedureType = procRs.getInt(prefix.concat("_TYPE"));
				switch(type)
				{
					case DatabaseMetaData.functionResultUnknown:
						returnTypeFlavor = ReturnTypeFlavor.UNKNOWN;
					break;
					// case DatabaseMetaData.functionNoTable:
					// returnTypeFlavor = ReturnTypeFlavor.;
					// break;
					// case DatabaseMetaData.functionReturnsTable:
					// returnTypeFlavor = ReturnTypeFlavor.;
					// break;
					default:
						returnTypeFlavor = ReturnTypeFlavor.UNKNOWN;
				}
				
			}
			else
			{
				type = resultSet.getInt(prefix.concat(TYPE));
				switch(type)
				{
					case DatabaseMetaData.procedureNoResult:
						returnTypeFlavor = ReturnTypeFlavor.VOID;
					break;
					
					default:
						returnTypeFlavor = ReturnTypeFlavor.UNKNOWN;
				}
			}
			
			// search for a procedure column
			if(resultMap.containsKey(name))
			{
				addStoredProceduresWithParams(list,resultMap,name,description,returnTypeFlavor,
						returnType,prefix);
			}
			else
			{
				// add without params
				list.add(new StoredProcedure(returnTypeFlavor,returnType,name,description,
						new Param[0]));
			}
			
		}
		resultSet.close();
	}
	
	
	private void addStoredProceduresWithParams(List<StoredProcedure> list,
			Map<String, List<JDBCColumnsMetaData>> resultMap, String procName, String description,
			ReturnTypeFlavor returnTypeFlavor, DataType returnType, String prefix)
	{
		
		if(procName != null)
		{
			
			List<JDBCColumnsMetaData> metaDataList = resultMap.get(procName);
			List<Param> params = new ArrayList<Param>();
			for(JDBCColumnsMetaData jdbcColumnsMetaData : metaDataList)
			{
				
				if(jdbcColumnsMetaData.getColumnName() != null)
				{
					
					if(prefix.equals(FUNCTION))
					{
						switch(jdbcColumnsMetaData.getColumnType())
						{
							case DatabaseMetaData.functionReturn:
								returnTypeFlavor = ReturnTypeFlavor.TYPE;
								returnType = jdbcColumnsMetaData.getDataType();
							break;
							
							case DatabaseMetaData.functionColumnResult:
								returnTypeFlavor = ReturnTypeFlavor.RESULT_SET;
							break;
							
							case DatabaseMetaData.functionColumnIn:
								params.add(new Param(ParamType.IN,jdbcColumnsMetaData
										.getColumnName(),jdbcColumnsMetaData.getDataType()));
							break;
							
							case DatabaseMetaData.functionColumnOut:
								params.add(new Param(ParamType.OUT,jdbcColumnsMetaData
										.getColumnName(),jdbcColumnsMetaData.getDataType()));
							break;
							
							case DatabaseMetaData.functionColumnInOut:
								params.add(new Param(ParamType.IN_OUT,jdbcColumnsMetaData
										.getColumnName(),jdbcColumnsMetaData.getDataType()));
							break;
							case DatabaseMetaData.functionColumnUnknown:
							
							break;
						}
					}
					else
					{
						switch(jdbcColumnsMetaData.getColumnType())
						{
							case DatabaseMetaData.procedureColumnReturn:
								returnTypeFlavor = ReturnTypeFlavor.TYPE;
								returnType = jdbcColumnsMetaData.getDataType();
							break;
							
							case DatabaseMetaData.procedureColumnResult:
								returnTypeFlavor = ReturnTypeFlavor.RESULT_SET;
							break;
							
							case DatabaseMetaData.procedureColumnIn:
								params.add(new Param(ParamType.IN,jdbcColumnsMetaData
										.getColumnName(),jdbcColumnsMetaData.getDataType()));
							break;
							
							case DatabaseMetaData.procedureColumnOut:
								params.add(new Param(ParamType.OUT,jdbcColumnsMetaData
										.getColumnName(),jdbcColumnsMetaData.getDataType()));
							break;
							
							case DatabaseMetaData.procedureColumnInOut:
								params.add(new Param(ParamType.IN_OUT,jdbcColumnsMetaData
										.getColumnName(),jdbcColumnsMetaData.getDataType()));
							break;
						}
					}
				}
				else
				{
					returnType = jdbcColumnsMetaData.getDataType();
				}
				
			}
			list.add(new StoredProcedure(returnTypeFlavor,returnType,procName,description,params
					.toArray(new Param[params.size()])));
		}
		
	}
	
	
	private String getTableTypeStatement(EnumSet<TableType> types)
	{
		
		if(types == null || types.size() <= 0)
		{
			return "";
		}
		
		String tableStatement = "(";
		
		if(types.contains(TableType.TABLE))
		{
			tableStatement += "type = 'T'";
		}
		
		if(types.contains(TableType.TABLE) && types.contains(TableType.VIEW))
		{
			tableStatement += " or ";
		}
		
		if(types.contains(TableType.VIEW))
		{
			tableStatement += "type = 'V'";
		}
		
		tableStatement += ")";
		return tableStatement;
	}
	
	
	@Override
	protected void createTable(JDBCConnection jdbcConnection, TableMetaData table)
			throws DBException, SQLException
	{
	}
	
	
	@Override
	protected void addColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column, ColumnMetaData columnBefore, ColumnMetaData columnAfter)
			throws DBException, SQLException
	{
	}
	
	
	@Override
	protected void alterColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column, ColumnMetaData existing) throws DBException, SQLException
	{
	}
	
	
	@Override
	public boolean equalsType(ColumnMetaData clientColumn, ColumnMetaData dbColumn)
	{
		return false;
	}
	
	
	@Override
	protected void dropColumn(JDBCConnection jdbcConnection, TableMetaData table,
			ColumnMetaData column) throws DBException, SQLException
	{
	}
	
	
	@Override
	protected void createIndex(JDBCConnection jdbcConnection, TableMetaData table, Index index)
			throws DBException, SQLException
	{
	}
	
	
	@Override
	protected void dropIndex(JDBCConnection jdbcConnection, TableMetaData table, Index index)
			throws DBException, SQLException
	{
	}
}
