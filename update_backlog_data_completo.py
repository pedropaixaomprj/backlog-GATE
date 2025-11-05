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
WITH bl_atual AS (
  select
    "NT_EXTENSO" "NUCLEO",
    "SEI"        "PROCEDIMENTO_SEI",
    "SAT"        "SAT",
    TO_CHAR("ENTRADA", 'DD/MM/YYYY') "DATA_ENVIO",
    coalesce(date_part('day', NOW() - "ENTRADA"),0) "DIAS_ENVIO",
    "PRIORIDADE",
    "TIPO_PRAZO" "TIPO PRAZO",
    TO_CHAR("PRAZO", 'DD/MM/YYYY') "DT PRAZO",
    "TP_EXTENSO" "TP",
    "NUM_MPRJ",
    "ORGAO_SOLICITANTE_EXTENSO" "ORGAO_SOLICITANTE",
    CASE WHEN "TEMAS" IS NOT NULL THEN "TEMAS" ELSE '### SEM TEMA ###' END "TEMAS"
  from stage."MVW_SEI_SAT_GESTAO_ACERVO_ADMISS"
  union all
  select
    "NT_EXTENSO", "SEI", "SAT",
    TO_CHAR("ENTRADA", 'DD/MM/YYYY'),
    coalesce(date_part('day', NOW() - "ENTRADA"),0),
    "PRIORIDADE",
    "TIPO_PRAZO",
    TO_CHAR("PRAZO", 'DD/MM/YYYY'),
    '_SEM USUARIO ATRIBUIDO',
    "NUM_MPRJ",
    "ORGAO_SOLICITANTE_EXTENSO",
    CASE WHEN "TEMAS" IS NOT NULL THEN "TEMAS" ELSE '### SEM TEMA ###' END
  from stage."MVW_SEI_SAT_GESTAO_ACERVO_ADMITIDO"
  union all
  select
    "NT_EXTENSO", "SEI", "SAT",
    TO_CHAR("ENTRADA", 'DD/MM/YYYY'),
    coalesce(date_part('day', NOW() - "ENTRADA"),0),
    "PRIORIDADE",
    "TIPO_PRAZO",
    TO_CHAR("PRAZO", 'DD/MM/YYYY'),
    "TP_ATRIBUIDO_EXTENSO",
    "NUM_MPRJ",
    "ORGAO_SOLICITANTE_EXTENSO",
    CASE WHEN "TEMAS" IS NOT NULL THEN "TEMAS" ELSE '### SEM TEMA ###' END
  from stage."MVW_SEI_SAT_GESTAO_ACERVO_DISTRIB"
)
select
  string_agg(DISTINCT "NUCLEO", ', ') AS "NUCLEO",
  "PROCEDIMENTO_SEI",
  "SAT",
  "DATA_ENVIO",
  "DIAS_ENVIO",
  "PRIORIDADE",
  "TIPO PRAZO",
  "DT PRAZO",
  string_agg(DISTINCT "TP", ', ') AS "TP",
  "NUM_MPRJ",
  "ORGAO_SOLICITANTE",
  "TEMAS"
FROM bl_atual
GROUP BY
  "PROCEDIMENTO_SEI","SAT","DATA_ENVIO","DIAS_ENVIO",
  "PRIORIDADE","TIPO PRAZO","DT PRAZO",
  "NUM_MPRJ","ORGAO_SOLICITANTE","TEMAS"
ORDER BY "DIAS_ENVIO" DESC, "PROCEDIMENTO_SEI" DESC;
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

