/*
 * SqlEngine Database Adapter DB2 - XAPI SqlEngine Database Adapter for DB2
 * Copyright © 2003 XDEV Software (https://xdev.software)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package xdev.db.db2.jdbc;

import com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor;
import com.xdev.jadoth.sqlengine.dbms.SQLExceptionParser;
import com.xdev.jadoth.sqlengine.internal.DatabaseGateway;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity;



public class DB2Dbms
		extends
		DbmsAdaptor.Implementation<DB2Dbms, DB2DMLAssembler, DB2DDLMapper, DB2RetrospectionAccessor, DB2Syntax>
{
	// /////////////////////////////////////////////////////////////////////////
	// constants //
	// ///////////////////
	
	/** The Constant MAX_VARCHAR_LENGTH. */
	protected static final int		MAX_VARCHAR_LENGTH		= Integer.MAX_VALUE;
	
	protected static final char		IDENTIFIER_DELIMITER	= '"';
	
	public static final DB2Syntax	SYNTAX					= new DB2Syntax();
	
	
	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////////
	
	public DB2Dbms()
	{
		this(new SQLExceptionParser.Body());
	}
	
	
	/**
	 * Instantiates a new db2 dbms.
	 *
	 * @param sqlExceptionParser
	 *            the sql exception parser
	 */
	public DB2Dbms(final SQLExceptionParser sqlExceptionParser)
	{
		super(sqlExceptionParser,false);
		this.setRetrospectionAccessor(new DB2RetrospectionAccessor(this));
		this.setDMLAssembler(new DB2DMLAssembler(this));
		this.setSyntax(SYNTAX);
	}
	
	
	/**
	 * @param host
	 * @param port
	 * @param user
	 * @param password
	 * @param catalog
	 * @param properties
	 * @return
	 *
	 * @see DbmsAdaptor#createConnectionInformation(String, int, String, String, String, String)
	 */
	@Override
	public DB2ConnectionInformation createConnectionInformation(final String host, final int port,
			final String user, final String password, final String catalog, final String properties)
	{
		return new DB2ConnectionInformation(host,port,user,password,catalog,properties, this);
	}
	
	
	/**
	 * @param table the table
	 * @return the object
	 */
	@Override
	public Object updateSelectivity(final SqlTableIdentity table)
	{
		return null;
	}
	
	
	/**
	 * @param bytes
	 * @param sb
	 * @return
	 * @see DbmsAdaptor#assembleTransformBytes(byte[],
	 *      java.lang.StringBuilder)
	 */
	@Override
	public StringBuilder assembleTransformBytes(final byte[] bytes, final StringBuilder sb)
	{
		return null;
	}
	
	
	/**
	 * @return
	 * @see DbmsAdaptor.Implementation#getRetrospectionAccessor()
	 */
	@Override
	public DB2RetrospectionAccessor getRetrospectionAccessor()
	{
		throw new RuntimeException("DB2 Retrospection not implemented yet!");
	}
	
	
	/**
	 * @param dbc
	 * @see DbmsAdaptor#initialize(com.xdev.jadoth.sqlengine.internal.DatabaseGateway)
	 */
	@Override
	public void initialize(final DatabaseGateway<DB2Dbms> dbc)
	{
	}
	
	
	/**
	 * @param fullQualifiedTableName
	 * @return
	 * @see DbmsAdaptor#rebuildAllIndices(java.lang.String)
	 */
	@Override
	public Object rebuildAllIndices(final String fullQualifiedTableName)
	{
		return null;
	}
	
	
	@Override
	public boolean supportsOFFSET_ROWS()
	{
		return false;
	}
	
	
	/**
	 * @return
	 * @see DbmsAdaptor#getMaxVARCHARlength()
	 */
	@Override
	public int getMaxVARCHARlength()
	{
		return MAX_VARCHAR_LENGTH;
	}
	
	
	@Override
	public char getIdentifierDelimiter()
	{
		return IDENTIFIER_DELIMITER;
	}
}
