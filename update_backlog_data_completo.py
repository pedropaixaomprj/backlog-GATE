import xlsxwriter
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()  # loads .env in development

conn = psycopg2.connect(
    host=os.environ.get("DB_HOST"),
    database=os.environ.get("DB_NAME"),
    user=os.environ.get("DB_USER"),
    password=os.environ.get("DB_PASSWORD"),
    port=os.environ.get("DB_PORT"))

conn.autocommit = True

cursor = conn.cursor()

query_backlog = '''
with pct as (
	select t."ID_PROTOCOLO",
	string_agg(distinct t."TEMA", ',') "TEMAS"
	from stage."VW_SEI_SAT_PROC_TEMA" t
	group by t."ID_PROTOCOLO"
	),
	csagr as (
	select "ID_PROTOCOLO" ,
	string_agg(distinct "ORGAO_DESTINO", ',') "NTs" ,
	string_agg(distinct "USUARIO_ATRIBUIDO", ',') "TPs"
	from stage."VW_SEI_Carga_SAT"
	where "MOVIMENTO_LOCAL" = 'No GATE'
	group by "ID_PROTOCOLO"
	)
	select distinct csagr."NTs" "NUCLEO" ,
					"PROCEDIMENTO_SEI" ,
					"ID_DOCUMENTO" "SAT",
					TO_CHAR("DATA_ENVIO_GATE", 'DD/MM/YYYY') "DATA_ENVIO",
					coalesce(date_part('day', NOW() - "DATA_ENVIO_GATE"),0) "DIAS_ENVIO",
					-- "DIAS_ENVIO_GATE" "DIAS_ENVIO", não funciona, não está batendo com "DATA_ENVIO_GATE"
					-- TO_CHAR("DATA_CRIACAO_SAT", 'DD/MM/YYYY') "DATA_CRIACAO_SAT",
					-- coalesce(date_part('day', NOW() - "DATA_CRIACAO_SAT"),0) "DIAS_CRIACAO_SAT",
					"MARCA_PRIOR_ATUAL" "PRIORIDADE", 
					"TIPO_PRAZO" "TIPO PRAZO",
					TO_CHAR("DATA_PRESCRICAO", 'DD/MM/YYYY') "DT PRAZO",
					csagr."TPs" "TP",
					"NUM_MPRJ" ,
					"ORGAO_SOLICITANTE" ,
						-- ,"DATA_MOVIMENTO" "DATA_ATRIBUICAO" ,
					case
						when pct."TEMAS" is not null then pct."TEMAS"
						else '### SEM TEMA ###'
					end "TEMAS"
					from stage."VW_SEI_Carga_SAT" cs
					left join csagr on csagr."ID_PROTOCOLO" = cs."ID_PROTOCOLO"
					left join pct on pct."ID_PROTOCOLO" = cs."ID_PROTOCOLO"
					where "MOVIMENTO_LOCAL" = 'No GATE'
					order by "DIAS_ENVIO" desc, "PROCEDIMENTO_SEI"
'''

query_prod = '''
select "SEI" "PROCEDIMENTO_SEI",
	   "SAT",
	   "NUM_MPRJ",
	   "NUMERO_IT" "NUM_IT",
	   TO_CHAR("ENTRADA", 'DD/MM/YYYY') "ENTRADA",
	   TO_CHAR("ADMISSAO", 'DD/MM/YYYY') "ADMISSAO",
	   TO_CHAR("DISTRIBUICAO", 'DD/MM/YYYY') "DISTRIBUICAO",
	   TO_CHAR("CRIACAO_IT", 'DD/MM/YYYY') "CRIACAO_IT",
	   "DIAS_ADMISS",
	   "DIAS_FILA",
	   "DIAS_PROD_IT",
	   "DIAS_TOTAL",
	   "TIPO_PRAZO",
	   "ORGAO_SOLICITANTE_EXTENSO" "ORGAO_SOLICITANTE",
	   "TEMAS",
	   "EQUIPE_EXTENSO",
	   "COMPLEMENTACAO",
	   "DUVIDA",
	   "LINK",
	   "LINK_SAT",
	   "LINK_IT"
from stage."MVW_SEI_SAT_GESTAO_ACERVO_PROD"
'''

query_saida_sem_it = '''
select  "SEI" "PROCEDIMENTO_SEI",
		"SAT",
		TO_CHAR("ENTRADA", 'DD/MM/YYYY')"ENTRADA",
		TO_CHAR("ADMISSAO", 'DD/MM/YYYY') "ADMISSAO",
		TO_CHAR("DISTRIBUICAO", 'DD/MM/YYYY') "DISTRIBUICAO",
		"SAIDA",
		"TIPO_PRAZO",
		"TEMAS",
		"ORGAO_SOLICITANTE_EXTENSO" "ORGAO_SOLICITANTE",
		"COMPLEMENTACAO",
		"DUVIDA",
		"LINK_SAT"
		from stage."MVW_SEI_SAT_BL_PASSADO"
		where "CRIACAO_IT" is NULL and "SAIDA" is not null and date_part('year',"SAIDA") >= 2025
'''

backlog = pd.read_sql(query_backlog, conn)
prod = pd.read_sql(query_prod, conn)
saida_sem_it = pd.read_sql(query_saida_sem_it, conn)

# Overwrite the file entirely with the new sheet

excel_path_backlog = '~pentaho/python/backlog-GATE/BacklogGATEAutoCompleto.xlsx'
excel_path_prod = '~pentaho/python/backlog-GATE/ProdGATEAutoCompleto.xlsx'
excel_path_saida_sem_it = '~pentaho/python/backlog-GATE/SaidaSemITGATEAutoCompleto.xlsx'
backlog.to_excel(excel_path_backlog, sheet_name='BacklogGATE', index=False, engine='xlsxwriter')
prod.to_excel(excel_path_prod, sheet_name='ProdGATE', index=False, engine='xlsxwriter')
saida_sem_it.to_excel(excel_path_saida_sem_it, sheet_name='SaidaSemITGATE', index=False, engine='xlsxwriter')

