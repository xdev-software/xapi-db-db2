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

import com.xdev.jadoth.sqlengine.SELECT;
import com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler;


public class DB2DMLAssembler extends StandardDMLAssembler<DB2Dbms>
{
	protected static final String _FETCH_FIRST_ = " FETCH FIRST ";
	
	public DB2DMLAssembler(final DB2Dbms dbms)
	{
		super(dbms);
	}
	
	/**
	 * @param query
	 * @param sb
	 * @param flags
	 * @param clauseSeperator
	 * @param newLine
	 * @param indentLevel
	 * @return
	 * @see StandardDMLAssembler#assembleSelectRowLimit(SELECT, StringBuilder, int, String, String, int)
	 */
	@Override
	protected StringBuilder assembleSelectRowLimit(
		final SELECT        query,
		final StringBuilder sb,
		final int           flags,
		final String        clauseSeperator,
		final String        newLine,
		final int           indentLevel)
	{
		final Integer top = query.getFetchFirstRowCount();
		if(top != null)
		{
			sb.append(_FETCH_FIRST_).append(top).append(_ROWS_ONLY);
		}
		return sb;
	}
}
