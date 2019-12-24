            select 
                wa.transaction_date,
                count(*) as rec_cnt_per_day
            from 
                dx.dx_architect_wam wa
            where 
                wa.architect_productline is not null
            and
                wa.architect_productline in ( '115', '116', '117' )
            and 
                '2019-10-01' <= wa.transaction_date
            and 
                wa.transaction_date <= '2019-10-31'
            group by
                wa.transaction_date
            order by
                wa.transaction_date asc
