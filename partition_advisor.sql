create procedure partition_advisor (l_table varchar, l_table_schema varchar) as $$
declare
    l_size_table int8 := 0; -- размер таблицы
    l_unique_cnt_column int8 := 0; --количество уникальных значений
    l_query varchar; -- запрос
    l_column varchar; -- колонки
    l_columns varchar[]; -- массив колонок
    l_unique_column varchar; -- количество уникальных записей в колонке
    l_unique_columns varchar[]; -- количество уникальных записей в колонке
    l_data_type varchar; -- тип данных колонки
    l_column_is_nullable varchar; -- допускается ли null в колонке
    l_most_column_attr varchar; -- столбец, который часто используется в where
    l_column_info record; -- колонки таблицы для пересоздания таблицы
    l_create_table_sql varchar := ''; -- для пересоздания таблиц
    l_value varchar; -- значение колонки
    l_constraint_name varchar; -- название ограничения
    l_foreign_key_table_r record; --ключи, которые зависят от анализируемой таблицы
    l_foreign_key_table record; --ключи анализируемой таблицы
    l_min_date date; -- минимальная дата в столбце с датами
    l_max_date date; -- максимальная дата в столбце с датами
    l_partition_name varchar; -- название создаваемой партиции
	l_partition_start_date date; -- минимальная дата в столбце с датами
	l_partition_end_date date; -- максимальная дата в столбце с датами
	l_dif_date int8; -- разница между максимальной и минимальной датой в столбце (в днях)
	l_interval INTERVAL; -- интервал для партиционирования по диапазону значений
    
    --множество ключей, которые зависят от анализируемой таблицы
    l_foreign_key_table_rs cursor for select distinct 
					    tc.table_schema as foreign_schema_name,
					    tc.table_name as foreign_table_name,
					    kcu.column_name as foreign_column_name,
					    ccu.table_schema,
					    ccu.table_name,
					    ccu.column_name,
					    tc.constraint_name
					from
					    information_schema.table_constraints as tc
					    join information_schema.key_column_usage as kcu
					      on tc.constraint_name = kcu.constraint_name
					    join information_schema.constraint_column_usage as ccu
					      on ccu.constraint_name = tc.constraint_name
					where
						tc.constraint_type = 'FOREIGN KEY'
						and ccu.table_name = l_table
						and ccu.table_schema = l_table_schema; 
    
	--множество ключей анализируемой таблицы
	l_foreign_key_tables cursor for select distinct
					    tc.table_schema as foreign_schema_name,
					    tc.table_name as foreign_table_name,
					    kcu.column_name as foreign_column_name,
					    ccu.table_schema,
					    ccu.table_name,
					    ccu.column_name,
					    tc.constraint_name
					from
					    information_schema.table_constraints as tc
					    join information_schema.key_column_usage as kcu
					      on tc.constraint_name = kcu.constraint_name
					    join information_schema.constraint_column_usage as ccu
					      on ccu.constraint_name = tc.constraint_name
					where
						tc.constraint_type = 'FOREIGN KEY'
						and tc.table_name = l_table
						and tc.table_schema = l_table_schema; 
begin	
	raise notice 'Начало анализа для таблицы %.%', l_table_schema, l_table;
	raise notice '';	
    raise notice '----------------------------------------------------------------------------';
    raise notice '';
	
	l_size_table = pg_relation_size(l_table_schema || '.' || l_table);

	if l_size_table = 0 then 
		raise notice 'Таблица уже является партиционированной';
	elsif l_size_table < 2147483648 then
		raise notice 'Партиционирование не требуется';
	else
		l_query := 'select column_name, data_type, is_nullable
			        from information_schema.columns
			        where table_name = ''' || l_table || '''
			        	and table_schema = ''' || l_table_schema || ''''; --определяем колонки таблицы
		        
		for l_column in execute l_query
		loop
		    l_columns = array_append(l_columns, l_column);
		end loop;
	
		l_query := 'select attname
					from pg_stats
					where tablename = ''' || l_table || '''
						and schemaname = ''' || l_table_schema || '''
						and most_common_freqs is not null
					order by most_common_freqs desc 
					limit 1';
		
		execute l_query into l_most_column_attr;
					       
		--анализируем каждую колонку
		for i in 1..array_length(l_columns, 1) loop     
		    
			l_query := 'select count(distinct ' || l_columns[i] || ') from ' || l_table_schema || '.' || l_table;
			execute l_query into l_unique_cnt_column;	
		   
			l_query := 'select data_type, is_nullable
                  from information_schema.columns
                  where table_name = ''' || l_table || '''
					and table_schema = ''' || l_table_schema || '''
                  	and column_name = ''' || l_columns[i] || ''''; --получаем тип данных и допускается ли null для колонки
      		
            execute l_query into l_data_type, l_column_is_nullable;
           
           --если количество уникальных значений маленькое
			if l_unique_cnt_column <= 20 then
				raise notice 'Рекомендуется использовать партиционирование по значению колонки: %', l_columns[i];
			
           		if l_columns[i] = l_most_column_attr then
					raise notice '(Предподчительный вариант, потому то столбец % часто используется в where)', l_columns[i];
           		end if;
           	
           		if l_column_is_nullable = 'YES' then
           			raise notice 'Значение колонки может быть null, в связи с этим может быть перекос данных';
           		end if;
           		
				raise notice '';
				raise notice 'Код для изменения таблицы на партиционированную:';
				raise notice '';
			
           		--код для создания таблицы с партиционированием
           		--отключаем все триггеры
           	
           		raise notice 'alter table %.% disable trigger all;', l_table_schema, l_table;
           	
           		l_create_table_sql := ' ';
           	
           		--удаляем все ключи, которые зависят от анализируемой таблицы
           		for l_foreign_key_table_r in 
           			select distinct
					    tc.constraint_name as constraint_name
					from
					    information_schema.table_constraints as tc
					    join information_schema.constraint_column_usage as ccu
					      on ccu.constraint_name = tc.constraint_name
					where
						tc.constraint_type = 'FOREIGN KEY'
						and ccu.table_name = l_table
						and ccu.table_schema = l_table_schema
				loop
					raise notice 'alter table %.% drop constraint %;', l_table_schema, l_table, l_foreign_key_table_r.constraint_name;
				end loop;
				
           		raise notice '';
           	
				--удаляем все ключи, анализируемой таблицы
           		for l_foreign_key_table in 
           			select distinct
					    tc.constraint_name as constraint_name
					from
					    information_schema.table_constraints as tc
					where
						tc.constraint_type = 'FOREIGN KEY'
						and tc.table_name = l_table
						and tc.table_schema = l_table_schema
				loop
					raise notice 'alter table %.% drop constraint %;', l_table_schema, l_table, l_foreign_key_table.constraint_name;
				end loop;
			
           		raise notice '';
           	
				raise notice 'create table %.%_tmp as', l_table_schema, l_table;
				raise notice 'select * from %.%;', l_table_schema, l_table;
			
				raise notice '';
			
				raise notice 'drop table %.%;', l_table_schema, l_table;
			
				raise notice '';
				
				raise notice 'create table %.%', l_table_schema, l_table;
				raise notice '(';
			
				--получение информации по столбцам
				for l_column_info in
			        select column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable, column_default
			        from information_schema.columns
			        where table_name = l_table
			        	and table_schema = l_table_schema
			    loop
			        -- добавление определения столбца к sql-запросу для создания таблицы
			        l_create_table_sql := l_create_table_sql || l_column_info.column_name || ' ' || l_column_info.data_type;
			        
			        -- если столбец имеет максимальную длину, добавляем это в определение
			        if l_column_info.character_maximum_length is not null then
			            l_create_table_sql := l_create_table_sql || '(' || l_column_info.character_maximum_length || ')';
			        end if;
			        
			        -- если столбец является числовым и имеет точность и масштаб, добавляем их в определение
			        if l_column_info.data_type in ('numeric', 'decimal') then
			            l_create_table_sql := l_create_table_sql || '(' || l_column_info.numeric_precision || ',' || l_column_info.numeric_scale || ')';
			        end if;
			        
			        -- добавление информации о null или not null к определению столбца
			        if l_column_info.is_nullable = 'no' then
			            l_create_table_sql := l_create_table_sql || ' not null';
			        end if;
			        
			        -- добавление значения по умолчанию, если оно существует
			        if l_column_info.column_default is not null then
			            l_create_table_sql := l_create_table_sql || ' default ' || l_column_info.column_default;
			        end if;
			        
			        -- добавление разделителя для следующего столбца
			        l_create_table_sql := l_create_table_sql || ', ' || chr(10);
			    end loop;
				
			    l_create_table_sql := substring(l_create_table_sql, 2, length(l_create_table_sql) - 4);
			   	raise notice '%', l_create_table_sql;
			    raise notice ')';
			    raise notice 'partition by list (%);', l_columns[i];
			   
			   --создание партиций
			   
			    l_query := 'select distinct ' ||  l_columns[i] ||
			    			' from ' || l_table_schema || '.' || l_table; --уникальные значения в колонке
			    
			    for l_unique_column in execute l_query
				loop
					raise notice 'create table %.%_% partition of %.%', 
						l_table_schema, l_table, l_unique_column, l_table_schema, l_table;
					
					raise notice 'for values in (''%'');', l_unique_column;
				end loop;	
			
			    raise notice '';
			
				raise notice 'insert into %.%', l_table_schema, l_table;
				raise notice 'select * from %.%_tmp;', l_table_schema, l_table;
           		raise notice 'commit;';
				raise notice 'drop table %.%_tmp;', l_table_schema, l_table;
           		
           		raise notice '';	
           	
           		--добавленяем ключи, которые зависят от анализируемой таблицы
				for l_foreign_key_table_r in l_foreign_key_table_rs loop
			        raise notice 'alter table %.% add constraint %',
				                   l_foreign_key_table_r.foreign_schema_name,
				                   l_foreign_key_table_r.foreign_table_name,
				                   l_foreign_key_table_r.constraint_name;
				                  
			        raise notice 'foreign key (%) references %.%(%);',
				                   l_foreign_key_table_r.foreign_column_name,
				                   l_foreign_key_table_r.table_schema,
				                   l_foreign_key_table_r.table_name,
				                   l_foreign_key_table_r.column_name;
			    end loop;
			    
           		--добавленяем ключи анализируемой таблицы
			    for l_foreign_key_table in l_foreign_key_tables loop
				    raise notice 'alter table %.% add constraint %',
				                   l_foreign_key_table.foreign_schema_name,
				                   l_foreign_key_table.foreign_table_name,
				                   l_foreign_key_table.constraint_name;
				                  
				    raise notice '    foreign key (%) references %.%(%);',
				                   l_foreign_key_table.foreign_column_name,
				                   l_foreign_key_table.table_schema,
				                   l_foreign_key_table.table_name,
				                   l_foreign_key_table.column_name;
				end loop;
				
           		raise notice '';	
           		raise notice '----------------------------------------------------------------------------';
           		raise notice '';
           	
			--если это дата и количество уникальных значений большое
			elsif l_data_type = 'date' and
	            l_unique_cnt_column >= 365 then
				raise notice 'Рекомендуется использовать партиционирование по диапазону: %', l_columns[i];
			
           		if l_columns[i] = l_most_column_attr then
					raise notice '(Предподчительный вариант, потому то столбец % часто используется в where)', l_columns[i];
           		end if;    
           	
           		if l_column_is_nullable = 'YES' then
           			raise notice 'Значение колонки может быть null, в связи с этим может быть перекос данных';
           		end if;       	
           		
				raise notice '';
				raise notice 'Код для изменения таблицы на партиционированную:';
				raise notice '';
				
           		--код для создания таблицы с партиционированием
           		--отключаем все триггеры
           	
           		raise notice 'alter table %.% disable trigger all;', l_table_schema, l_table;
           	
           		l_create_table_sql := ' ';
           	
           		--удаляем все ключи, которые зависят от анализируемой таблицы
           		for l_foreign_key_table_r in 
           			select distinct
					    tc.constraint_name as constraint_name
					from
					    information_schema.table_constraints as tc
					    join information_schema.constraint_column_usage as ccu
					      on ccu.constraint_name = tc.constraint_name
					where
						tc.constraint_type = 'FOREIGN KEY'
						and ccu.table_name = l_table
						and ccu.table_schema = l_table_schema
				loop
					raise notice 'alter table %.% drop constraint %;', l_table_schema, l_table, l_foreign_key_table_r.constraint_name;
				end loop;
				
           		raise notice '';
           	
				--удаляем все ключи, анализируемой таблицы
           		for l_foreign_key_table in 
           			select distinct
					    tc.constraint_name as constraint_name
					from
					    information_schema.table_constraints as tc
					where
						tc.constraint_type = 'FOREIGN KEY'
						and tc.table_name = l_table
						and tc.table_schema = l_table_schema
				loop
					raise notice 'alter table %.% drop constraint %;', l_table_schema, l_table, l_foreign_key_table.constraint_name;
				end loop;
			
           		raise notice '';
			
				raise notice 'create table %.%_tmp as', l_table_schema, l_table;
				raise notice 'select * from %.%;', l_table_schema, l_table;
			
				raise notice '';
			
				raise notice 'drop table %.%;', l_table_schema, l_table;
			
				raise notice '';
				
				raise notice 'create table %.%', l_table_schema, l_table;
				raise notice '(';
			
				--получение информации по столбцам
				for l_column_info in
			        select column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable, column_default
			        from information_schema.columns
			        where table_name = l_table
			        	and table_schema = l_table_schema
			    loop
			        -- добавление определения столбца к sql-запросу для создания таблицы
			        l_create_table_sql := l_create_table_sql || l_column_info.column_name || ' ' || l_column_info.data_type;
			        
			        -- если столбец имеет максимальную длину, добавляем это в определение
			        if l_column_info.character_maximum_length is not null then
			            l_create_table_sql := l_create_table_sql || '(' || l_column_info.character_maximum_length || ')';
			        end if;
			        
			        -- если столбец является числовым и имеет точность и масштаб, добавляем их в определение
			        if l_column_info.data_type in ('numeric', 'decimal') then
			            l_create_table_sql := l_create_table_sql || '(' || l_column_info.numeric_precision || ',' || l_column_info.numeric_scale || ')';
			        end if;
			        
			        -- добавление информации о null или not null к определению столбца
			        if l_column_info.is_nullable = 'no' then
			            l_create_table_sql := l_create_table_sql || ' not null';
			        end if;
			        
			        -- добавление значения по умолчанию, если оно существует
			        if l_column_info.column_default is not null then
			            l_create_table_sql := l_create_table_sql || ' default ' || l_column_info.column_default;
			        end if;
			        
			        -- добавление разделителя для следующего столбца
			        l_create_table_sql := l_create_table_sql || ', ' || chr(10);
			    end loop;
				
			    l_create_table_sql := substring(l_create_table_sql, 2, length(l_create_table_sql) - 4);
			   	raise notice '%', l_create_table_sql;
			    raise notice ')';
			    raise notice 'partition by range (%);', l_columns[i];
			   
			   --создание партиций
			   
				l_query := 'select min(' || l_columns[i] || '), max(' || l_columns[i] || ') from ' || l_table_schema || '.' || l_table;
			    execute l_query into l_min_date, l_max_date;
			   
			   	l_dif_date = l_max_date - l_min_date;
			   
			    -- если разница меньше 3 лет, то диапазон будет 1 месяц
			   IF l_dif_date <= 1095 THEN
			   	l_interval := interval '1 month';
			    -- определим диапазоны дат и создадим соответствующие партиции
			    while l_min_date <= l_max_date loop
			        -- определяем границы диапазона (например, по месяцам)
			        l_partition_name := 'partition_' || to_char(l_min_date, 'YYYY') || '_' ||  to_char(l_min_date, 'MM');
			        l_partition_start_date := date_trunc('month', l_min_date);
			        l_partition_end_date := date_trunc('month', l_min_date) + l_interval;
			
			        -- создаем партицию для текущего диапазона дат
			        raise notice 'create table %.%_% partition of %.%', 
			                       l_table_schema, l_table, l_partition_name, 
			                       l_table_schema, l_table;
			                      
			        raise notice 'for values from (''%'') to (''%'');', 
			                       l_partition_start_date, l_partition_end_date;
			   
			        -- переходим к следующему диапазону
			        l_min_date := l_min_date + l_interval;
			    end loop;
			        l_partition_name := 'partition_' || to_char(l_min_date, 'YYYY') || '_' ||  to_char(l_min_date, 'MM');
			        l_partition_start_date := date_trunc('month', l_min_date);
			        l_partition_end_date := date_trunc('month', l_min_date) + l_interval;
			
			        -- создаем партицию для текущего диапазона дат
			        raise notice 'create table %.%_% partition of %.%', 
			                       l_table_schema, l_table, l_partition_name, 
			                       l_table_schema, l_table;
			                      
			        raise notice 'for values from (''%'') to (''%'');', 
			                       l_partition_start_date, l_partition_end_date;
			 
			    -- если разница от 3 до 20 лет, то диапазон будет 1 год
			   ELSIF l_dif_date > 1095 AND l_dif_date <= 7300 THEN
			    l_interval := interval '1 year';
			    -- определим диапазоны дат и создадим соответствующие партиции
			    while l_min_date <= l_max_date loop
			        -- определяем границы диапазона (например, по месяцам)
			        l_partition_name := 'partition_' || to_char(l_min_date, 'YYYY');
			        l_partition_start_date := date_trunc('year', l_min_date);
			        l_partition_end_date := date_trunc('year', l_min_date) + l_interval;
			
			        -- создаем партицию для текущего диапазона дат
			        raise notice 'create table %.%_% partition of %.%', 
			                       l_table_schema, l_table, l_partition_name, 
			                       l_table_schema, l_table;
			                      
			        raise notice 'for values from (''%'') to (''%'');', 
			                       l_partition_start_date, l_partition_end_date;
			   
			        -- переходим к следующему диапазону
			        l_min_date := l_min_date + l_interval;
			    end loop;
			        l_partition_name := 'partition_' || to_char(l_min_date, 'YYYY');
			        l_partition_start_date := date_trunc('year', l_min_date);
			        l_partition_end_date := date_trunc('year', l_min_date) + l_interval;
			
			        -- создаем партицию для текущего диапазона дат
			        raise notice 'create table %.%_% partition of %.%', 
			                       l_table_schema, l_table, l_partition_name, 
			                       l_table_schema, l_table;
			                      
			        raise notice 'for values from (''%'') to (''%'');', 
			                       l_partition_start_date, l_partition_end_date;
			   
			   	-- если разница больше 21 лет, то диапазон динамический
			   ELSIF l_dif_date > 7301 THEN
			   	l_interval := make_interval(years => round(l_dif_date / 365 / 20)::int);
			    -- определим диапазоны дат и создадим соответствующие партиции
			    while l_min_date <= l_max_date loop
			        -- определяем границы диапазона (например, по месяцам)
			        l_partition_name := 'partition_' || to_char(l_min_date, 'YYYY');
			        l_partition_start_date := date_trunc('year', l_min_date);
			        l_partition_end_date := date_trunc('year', l_min_date) + l_interval;
			
			        -- создаем партицию для текущего диапазона дат
			        raise notice 'create table %.%_% partition of %.%', 
			                       l_table_schema, l_table, l_partition_name, 
			                       l_table_schema, l_table;
			                      
			        raise notice 'for values from (''%'') to (''%'');', 
			                       l_partition_start_date, l_partition_end_date;
			   
			        -- переходим к следующему диапазону
			        l_min_date := l_min_date + l_interval;
			    end loop;
			        l_partition_name := 'partition_' || to_char(l_min_date, 'YYYY');
			        l_partition_start_date := date_trunc('year', l_min_date);
			        l_partition_end_date := date_trunc('year', l_min_date) + l_interval;
			
			        -- создаем партицию для текущего диапазона дат
			        raise notice 'create table %.%_% partition of %.%', 
			                       l_table_schema, l_table, l_partition_name, 
			                       l_table_schema, l_table;
			                      
			        raise notice 'for values from (''%'') to (''%'');', 
			                       l_partition_start_date, l_partition_end_date;
			   END IF;
			   
			    raise notice '';
			
				raise notice 'insert into %.%', l_table_schema, l_table;
				raise notice 'select * from %.%_tmp;', l_table_schema, l_table;
           		raise notice 'commit;';
				raise notice 'drop table %.%_tmp;', l_table_schema, l_table;
           		
           		raise notice '';	
           	
           		--добавленяем ключи, которые зависят от анализируемой таблицы
				for l_foreign_key_table_r in l_foreign_key_table_rs loop
			        raise notice 'alter table %.% add constraint %',
				                   l_foreign_key_table_r.foreign_schema_name,
				                   l_foreign_key_table_r.foreign_table_name,
				                   l_foreign_key_table_r.constraint_name;
				                  
			        raise notice 'foreign key (%) references %.%(%);',
				                   l_foreign_key_table_r.foreign_column_name,
				                   l_foreign_key_table_r.table_schema,
				                   l_foreign_key_table_r.table_name,
				                   l_foreign_key_table_r.column_name;
			    end loop;
			    
           		--добавленяем ключи анализируемой таблицы
			    for l_foreign_key_table in l_foreign_key_tables loop
				    raise notice 'alter table %.% add constraint %',
				                   l_foreign_key_table.foreign_schema_name,
				                   l_foreign_key_table.foreign_table_name,
				                   l_foreign_key_table.constraint_name;
				                  
				    raise notice '    foreign key (%) references %.%(%);',
				                   l_foreign_key_table.foreign_column_name,
				                   l_foreign_key_table.table_schema,
				                   l_foreign_key_table.table_name,
				                   l_foreign_key_table.column_name;
				end loop;
				
           		raise notice '';	
           		raise notice '----------------------------------------------------------------------------';
           		raise notice '';
           	
			--если очень много уникальных значений
	        elsif l_unique_cnt_column >= 100000 then
				raise notice 'Рекомендуется использовать партиционирование по хешу: %', l_columns[i];
				raise notice '(Использовать только в случае, если нет других вариантов)';
           	
           		if l_column_is_nullable = 'YES' then
           			raise notice 'Значение колонки может быть null, в связи с этим может быть перекос данных';
           		end if;
			
           		if l_columns[i] = l_most_column_attr then
					raise notice '(Предподчительный вариант, потому то столбец % часто используется в where)', l_columns[i];
           		end if;
           		
				raise notice '';
				raise notice 'Код для изменения таблицы на партиционированную:';
				raise notice '';
				
           		--код для создания таблицы с партиционированием
           		--отключаем все триггеры
           	
           		raise notice 'alter table %.% disable trigger all;', l_table_schema, l_table;
           	
           		l_create_table_sql := ' ';
           	
           		--удаляем все ключи, которые зависят от анализируемой таблицы
           		for l_foreign_key_table_r in 
           			select distinct
					    tc.constraint_name as constraint_name
					from
					    information_schema.table_constraints as tc
					    join information_schema.constraint_column_usage as ccu
					      on ccu.constraint_name = tc.constraint_name
					where
						tc.constraint_type = 'FOREIGN KEY'
						and ccu.table_name = l_table
						and ccu.table_schema = l_table_schema
				loop
					raise notice 'alter table %.% drop constraint %;', l_table_schema, l_table, l_foreign_key_table_r.constraint_name;
				end loop;
				
           		raise notice '';
           	
				--удаляем все ключи, анализируемой таблицы
           		for l_foreign_key_table in 
           			select distinct
					    tc.constraint_name as constraint_name
					from
					    information_schema.table_constraints as tc
					where
						tc.constraint_type = 'FOREIGN KEY'
						and tc.table_name = l_table
						and tc.table_schema = l_table_schema
				loop
					raise notice 'alter table %.% drop constraint %;', l_table_schema, l_table, l_foreign_key_table.constraint_name;
				end loop;
			
           		raise notice '';
			
				raise notice 'create table %.%_tmp as', l_table_schema, l_table;
				raise notice 'select * from %.%;', l_table_schema, l_table;
			
				raise notice '';
			
				raise notice 'drop table %.%;', l_table_schema, l_table;
			
				raise notice '';
				
				raise notice 'create table %.%', l_table_schema, l_table;
				raise notice '(';
			
				--получение информации по столбцам
				for l_column_info in
			        select column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable, column_default
			        from information_schema.columns
			        where table_name = l_table
			        	and table_schema = l_table_schema
			    loop
			        -- добавление определения столбца к sql-запросу для создания таблицы
			        l_create_table_sql := l_create_table_sql || l_column_info.column_name || ' ' || l_column_info.data_type;
			        
			        -- если столбец имеет максимальную длину, добавляем это в определение
			        if l_column_info.character_maximum_length is not null then
			            l_create_table_sql := l_create_table_sql || '(' || l_column_info.character_maximum_length || ')';
			        end if;
			        
			        -- если столбец является числовым и имеет точность и масштаб, добавляем их в определение
			        if l_column_info.data_type in ('numeric', 'decimal') then
			            l_create_table_sql := l_create_table_sql || '(' || l_column_info.numeric_precision || ',' || l_column_info.numeric_scale || ')';
			        end if;
			        
			        -- добавление информации о null или not null к определению столбца
			        if l_column_info.is_nullable = 'no' then
			            l_create_table_sql := l_create_table_sql || ' not null';
			        end if;
			        
			        -- добавление значения по умолчанию, если оно существует
			        if l_column_info.column_default is not null then
			            l_create_table_sql := l_create_table_sql || ' default ' || l_column_info.column_default;
			        end if;
			        
			        -- добавление разделителя для следующего столбца
			        l_create_table_sql := l_create_table_sql || ', ' || chr(10);
			    end loop;
				
			    l_create_table_sql := substring(l_create_table_sql, 2, length(l_create_table_sql) - 4);
			   	raise notice '%', l_create_table_sql;
			    raise notice ')';
			    raise notice 'partition by hash (%);', l_columns[i];
			   
			   --создание партиций
			   
			    for i in 0 .. 19
				loop
					raise notice 'create table %.%_% partition of %.%', 
						l_table_schema, l_table, i, l_table_schema, l_table;
					
					raise notice 'for values with (modulus 20, remainder %);', i;
				end loop;	
			   
			    raise notice '';
			
				raise notice 'insert into %.%', l_table_schema, l_table;
				raise notice 'select * from %.%_tmp;', l_table_schema, l_table;
           		raise notice 'commit;';
				raise notice 'drop table %.%_tmp;', l_table_schema, l_table;
           		
           		raise notice '';	
           	
           		--добавленяем ключи, которые зависят от анализируемой таблицы
				for l_foreign_key_table_r in l_foreign_key_table_rs loop
			        raise notice 'alter table %.% add constraint %',
				                   l_foreign_key_table_r.foreign_schema_name,
				                   l_foreign_key_table_r.foreign_table_name,
				                   l_foreign_key_table_r.constraint_name;
				                  
			        raise notice 'foreign key (%) references %.%(%);',
				                   l_foreign_key_table_r.foreign_column_name,
				                   l_foreign_key_table_r.table_schema,
				                   l_foreign_key_table_r.table_name,
				                   l_foreign_key_table_r.column_name;
			    end loop;
			    
           		--добавленяем ключи анализируемой таблицы
			    for l_foreign_key_table in l_foreign_key_tables loop
				    raise notice 'alter table %.% add constraint %',
				                   l_foreign_key_table.foreign_schema_name,
				                   l_foreign_key_table.foreign_table_name,
				                   l_foreign_key_table.constraint_name;
				                  
				    raise notice '    foreign key (%) references %.%(%);',
				                   l_foreign_key_table.foreign_column_name,
				                   l_foreign_key_table.table_schema,
				                   l_foreign_key_table.table_name,
				                   l_foreign_key_table.column_name;
				end loop;
				
           		raise notice '';	
           		raise notice '----------------------------------------------------------------------------';
           		raise notice '';
           	
	        end if;
			
		end loop;
		
	end if;
end;
$$ LANGUAGE plpgsql;
