--
-- Library
--
meekro = meekro or {}
meekro._debug = false
meekro._drivers = {}
meekro.error = "meekro-error"
meekro.lastInsert = 0
meekro._placeholders = {
	string = "s",
	boolean = "b",
	number = "f"
}

--
-- MySQLOO
--
meekro._drivers["mysqloo"] = {
	--
	-- Initialize
	--
	init = function()
		require("mysqloo")
	end,

	--
	-- Connect to the database
	--
	connect = function(host, username, password, database, port)
		local connection = mysqloo.connect(host, username, password, database, port or 3306)
		connection.onConnected = meekro.OnConnectSuccess
		connection.onConnectionFailed = meekro.OnConnectFailed
		connection:connect()
		
		meekro._connection = connection
	end,

	--
	-- Query the database
	--
	query = function(str)
		-- no error so far
		meekro.error = false

		-- create query
		local qry = meekro._connection:query(str)

		-- check for connection errors
		if not qry then
			error("[meekro] cannot query, not connected")
		end

		-- start and then wait
		qry:start()
		qry:wait()
		
		-- check for error
		if qry:error() ~= "" then
			meekro.error = true
			return qry:error()
		end

		-- set last insert
		meekro.lastInsert = qry:lastInsert()

		return qry:getData()
	end,
	
	--
	-- Disconnect from database
	--
	disconnect = function()
		MsgC(Color(255, 0, 0), "[meekro] driver does not implement disconnect\n")
	end,
	
	--
	-- Escape string
	--
	escape = function(str)
		return meekro._connection:escape(str)
	end,

	--
	-- Check if connected
	--
	connected = function()
		if meekro._connection:status() == mysqloo.DATABASE_CONNECTED then
			return true
		else
			return false
		end
	end
}

--
-- Properties
--
meekro.Driver = "mysqloo"
meekro.OnConnectSuccess = function() end
meekro.OnConnectFailed = function() end

--
-- Initialize the current driver
--
meekro.init = function()
	meekro.driver().init()
end

--
-- Connect to a MySQL database
--
meekro.connect = function(host, username, password, database, port)
	meekro.driver().connect(host, username, password, database, port)
end

--
-- Update the database
--
meekro.update = function(tbl, data, query, ...)
	-- setup variables
	local sets = {}

	-- extract data
	for k, v in pairs(data) do
		-- check key is a string
		if type(k) ~= "string" then
			error("[meekro] insert provided invalid key type")	
		end

		-- build set
		local set = "`" .. k .. "`="

		-- escape/convert types
		if type(v) == "string" then
			set = set .. "'" .. meekro.escape(v) .. "'"
		elseif type(v) == "boolean" then
			set = set .. tostring(v and 1 or 0)
		else
			set = set .. tostring(v)
		end

		-- insert set
		table.insert(sets, set)
	end

	-- build query
	local query = ("UPDATE `" .. tbl .. "` " ..
		"SET " .. string.Implode(",", sets) ..
		" WHERE " .. meekro.format(query, unpack({...})))

	-- debugging
	if meekro.debug then
		MsgC(Color(255, 127, 0), "[meekro] built update: " .. query .. "\n")
	end
	
	-- execute query
	return meekro.driver().query(query)
end

--
-- Insert into the database
--
meekro.insert = function(tbl, data)
	-- setup variables
	local fields = {}
	local values = {}

	-- extract data
	for k, v in pairs(data) do
		-- check key is a string
		if type(k) ~= "string" then
			error("[meekro] insert provided invalid key type")	
		end

		-- insert string
		table.insert(fields, meekro.escape(k))

		-- escape/convert types
		if type(v) == "string" then
			table.insert(values, "'" .. meekro.escape(v) .. "'")
		elseif type(v) == "boolean" then
			table.insert(values, tostring(v and 1 or 0))
		else
			table.insert(values, tostring(v))
		end
	end

	-- build query
	local query = ("INSERT INTO `" .. tbl .. "` " ..
		"(" .. string.Implode(",", fields) .. ") " ..
		"VALUES (" .. string.Implode(",", values) .. ")")

	-- debugging
	if meekro.debug then
		MsgC(Color(255, 127, 0), "[meekro] built insert: " .. query .. "\n")
	end
	
	-- execute query
	return meekro.driver().query(query)
end

--
-- Query more than one row
--
meekro.query = function(query, ...)
	-- build query
	local query = meekro.format(query, unpack({...}))

	-- debugging
	if meekro.debug then
		MsgC(Color(255, 127, 0), "[meekro] built query: " .. query .. "\n")
	end
	
	-- execute query
	return meekro.driver().query(query)
end

--
-- Query a single row
--
meekro.queryRow = function(query, ...)
	-- remove trailing semicolon
	query = string.TrimRight(query)
	query = string.TrimRight(query, ";")

	return meekro.query(query .. " LIMIT 1;", unpack({...}))
end

--
-- Delete data
--
meekro.delete = function(tbl, query, ...)
	-- build query
	local query = ("DELETE FROM `" .. tbl .. "` " ..
		" WHERE " .. meekro.format(query, unpack({...})))

	-- debugging
	if meekro.debug then
		MsgC(Color(255, 127, 0), "[meekro] built delete: " .. query .. "\n")
	end
	
	-- execute query
	return meekro.driver().query(query)
end

--
-- Drop a table
--
meekro.dropTable = function(tbl)
	-- build query
	local query = ("DROP TABLE `" .. tbl .. "`")

	-- debugging
	if meekro.debug then
		MsgC(Color(255, 127, 0), "[meekro] built table drop: " .. query .. "\n")
	end
	
	-- execute query
	return meekro.driver().query(query)
end

--
-- Drop a database
--
meekro.dropDatabase = function(db)
	-- build query
	local query = ("DROP DATABASE `" .. db .. "`")

	-- debugging
	if meekro.debug then
		MsgC(Color(255, 127, 0), "[meekro] built database drop: " .. query .. "\n")
	end
	
	-- execute query
	return meekro.driver().query(query)
end

--
-- Get if connected
--
meekro.connected = function()
	return meekro.driver().connected()
end

--
-- Get current driver table
--
meekro.driver = function()
	return meekro._drivers[meekro.Driver]
end

--
-- Escape a string
--
meekro.escape = function(str)
	return meekro.driver().escape(str)
end

--
-- Enable/disable debugging
--
meekro.debug = function(enabled)
	meekro._debug = enabled
end

--
-- Format a query string
--
meekro.format = function(str, ...)
	-- variables
	local state = "start"
	local out = ""
	local args = {...}
	
	-- current argument
	local j = 1
	
	-- placeholder
	local p = ""
	
	-- loop through string
	for i=1,str:len() do 
		-- character
		local c = str:sub(i,i)
		
		-- states
		if state == "start" then
			if c == "%" then
				-- check arguments
				if j > #args then
					error("[meekro] too few arguments passed to query")
				end
				
				-- transition to percent
				state = "percent"
				p = ""
			else
				-- append character
				out = out .. c
			end
		elseif state == "percent" then
			-- append placeholder
			p = p .. c
			
			-- check for automatic placeholder
			if p == "?" then
				if not meekro._placeholders[type(args[j])] then
					out = out .. "%?"
					state = "start"
					j = j + 1
					MsgC(Color(255, 0, 0), "[meekro] query uses invalid value")
				else
					p = meekro._placeholders[type(args[j])]
				end
			end
			
			-- replace with escaped string
			if p == "s" then
				out = out .. "'" .. tostring(meekro.escape(args[j])) .. "'"
				state = "start"
				j = j + 1
			elseif p == "i" then
				out = out .. math.floor(tonumber(args[j]))
				state = "start"
				j = j + 1
			elseif p == "d" then
				out = out .. tonumber(args[j])
				state = "start"
				j = j + 1
			elseif p == "b" then
				out = out .. (args[j] and 1 or 0)
				state = "start"
				j = j + 1
			elseif p == "l" then
				out = out .. tostring(args[j])
				state = "start"
				j = j + 1
			end
			
			-- check if no placeholders found
			if p:len() > 2 then
				out = out .. "%" .. p
				state = "start"
				j = j + 1
				MsgC(Color(255, 0, 0), "[meekro] query uses invalid placeholder")
			end
		end
	end
	
	return out
end
