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


import xdev.db.DBException;
import xdev.db.jdbc.JDBCDataSource;


public class DB2JDBCDataSource extends JDBCDataSource<DB2JDBCDataSource, DB2Dbms>
{
	public DB2JDBCDataSource()
	{
		super(new DB2Dbms());
	}
	
	
	@Override
	public Parameter[] getDefaultParameters()
	{
		return new Parameter[]{HOST.clone(),PORT.clone(50000),USERNAME.clone("db2admin"),
				PASSWORD.clone(),SCHEMA.clone(),CATALOG.clone(),URL_EXTENSION.clone(),
				IS_SERVER_DATASOURCE.clone(),SERVER_URL.clone(),AUTH_KEY.clone()};
	}
	
	
	@Override
	protected DB2ConnectionInformation getConnectionInformation()
	{
		return new DB2ConnectionInformation(getHost(),getPort(),getUserName(),getPassword()
				.getPlainText(),getCatalog(),getUrlExtension(),getDbmsAdaptor());
	}
	
	
	@Override
	public DB2JDBCConnection openConnectionImpl() throws DBException
	{
		return new DB2JDBCConnection(this);
	}
	
	
	@Override
	public DB2JDBCMetaData getMetaData() throws DBException
	{
		return new DB2JDBCMetaData(this);
	}
	
	
	@Override
	public boolean canExport()
	{
		return false;
	}
}
