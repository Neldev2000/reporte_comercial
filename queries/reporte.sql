with variables (sucursal, tiempo) as (values(10, 'month')),
contratos as (
	select 
		date_trunc( (select tiempo from variables), c.created_at )::date as fecha,
		count(*) as ventas 
	from contracts as c
	where 
		c.franchise_id = (select sucursal from variables)
	group by
		date_trunc( (select tiempo from variables), c.created_at )::date 
	order by 
		date_trunc( (select tiempo from variables), c.created_at )::date desc
	limit 1
	),
instalados as(
	select 
		date_trunc((select tiempo from variables), si.installation_date)::date as fecha,
		count(*) as instalaciones
	from 
		service_installation as si
	join 
		contracts as c 
			on (c.id = si.contract_id)
	where 
		c.franchise_id = (select sucursal from variables)
		and installation_date is not null
	group by
		date_trunc((select tiempo from variables), si.installation_date)::date
	order by
		date_trunc((select tiempo from variables), si.installation_date)::date desc
		limit 1),
	pending as (
		select 
			date_trunc((select tiempo from variables), now())::date as fecha,
			count(*) as pendientes
		from contracts as c 
		where 
			c.franchise_id = (select sucursal from variables) and
			c.status = 'awaiting_installation'
		)
		
select 
	now()::date as fecha,
	instalados.instalaciones as instalaciones_mes,
	contratos.ventas as ventas_mes,
	pending.pendientes as pendientes
from 
	instalados
join
	contratos using(fecha)
join
	pending using (fecha)
