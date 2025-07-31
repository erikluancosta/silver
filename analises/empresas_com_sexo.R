source('R/conectar.R')
library(vitaltable)
library(sf) # A função data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABIAAAASCAYAAABWzo5XAAAAbElEQVR4Xs2RQQrAMAgEfZgf7W9LAguybljJpR3wEse5JOL3ZObDb4x1loDhHbBOFU6i2Ddnw2KNiXcdAXygJlwE8OFVBHDgKrLgSInN4WMe9iXiqIVsTMjH7z/GhNTEibOxQswcYIWYOR/zAjBJfiXh3jZ6AAAAAElFTkSuQmCCst_read(con, query= query) carrega geometrias
library(tidyverse)

con <- conectar('receitaf')

# Retornou 1.894.892 empresas
empresas_sx <- dbGetQuery(
  con,
  "
  WITH est_filtrado AS (
      SELECT *
      FROM estabelecimento
      WHERE situacao_cadastral = 2
        AND uf IN ('AC','AM','AP','PA','RO','RR','TO', 'MT', 'MA')
        ),
  sexo_agg AS (
      SELECT 
        cnpj_basico,
        COUNT(*) FILTER (WHERE sexo = 'Masc' AND identificador_socio = 2) AS sx_masculino,
        COUNT(*) FILTER (WHERE sexo = 'Fem' AND identificador_socio = 2) AS sx_feminino,
        COUNT(*) FILTER (
          WHERE identificador_socio = 2 AND (sexo IS NULL OR sexo NOT IN ('Masc', 'Fem'))
        ) AS sx_ign
      FROM socios_nomes
      WHERE identificador_socio = 2
      GROUP BY cnpj_basico
    )
    SELECT 
      (es.cnpj_basico || es.cnpj_ordem || es.cnpj_dv) AS cnpj_completo,
      es.cnpj_basico,
      es.cnpj_ordem,
      es.cnpj_dv,
      es.nome_fantasia,
      em.razao_social,
      es.situacao_cadastral,
      es.data_situacao_cadastral,
      es.data_inicio_atividade,
      em.natureza_juridica,
      n.descricao as ds_natureza_juridica,
      es.cnae_fiscal_principal,
      c.descricao as ds_cnae_fiscal_principal,
      c.cnse2_denom as cnae_denom,
      c.cngr2_denom as cnae_grupo,
      c.cndv2_denom,
      em.porte_empresa,
      es.identificador_matriz_filial,
      em.capital_social,
      es.tipo_logradouro,
      es.logradouro,
      es.numero,
      es.complemento,
      es.bairro,
      es.cep,
      es.uf,
      mu.descricao as municipio_descricao,
      mu.cd_mun_7 as cd_ibge7,
      es.ddd_1,
      es.telefone_1,
      es.correio_eletronico,
      eg.sexo as sexo_mei,
      cg.latitude,
      cg.longitude,
      sx.sx_masculino,
      sx.sx_feminino,
      sx.sx_ign
    FROM est_filtrado es
    LEFT JOIN munic_comp mu ON es.municipio = mu.codigo
    LEFT JOIN empresa em ON es.cnpj_basico = em.cnpj_basico
    LEFT JOIN empresa_genero eg ON es.cnpj_basico = eg.cnpj_basico
    LEFT JOIN cnae_maior c ON es.cnae_fiscal_principal = c.codigo::NUMERIC
    LEFT JOIN natju n ON em.natureza_juridica = n.codigo
    LEFT JOIN cnpj_geocodificados cg 
      ON es.cnpj_basico = cg.cnpj_basico 
     AND es.cnpj_ordem = cg.cnpj_ordem 
     AND es.cnpj_dv = cg.cnpj_dv
    LEFT JOIN sexo_agg sx ON es.cnpj_basico = sx.cnpj_basico
    WHERE em.natureza_juridica IN (
      2151, 2291, 2143, 2305, 2011, 2135, 2178, 2062,
      2160, 2046, 2054, 2038, 2127, 2097, 2089, 2070
    );
  "
)


empresas_sx <- empresas_sx |> 
  mutate(
    rating_sexo_masc = sx_masculino / (sx_masculino + sx_feminino + sx_ign)*100,
    rating_sexo_fem = sx_feminino / (sx_masculino + sx_feminino + sx_ign)*100
)


empresas_def_sex <- empresas_sx |>
  mutate(sexo_final = case_when(
    (rating_sexo_fem >=51 | sexo_mei == "Fem") ~ "Fem",
    (rating_sexo_masc >=51 | sexo_mei == "Masc") ~ "Masc",
    (rating_sexo_masc == 50 & rating_sexo_fem == 50) ~ "Ambos",
    (is.na(sexo_mei) & rating_sexo_fem < 51 & rating_sexo_masc < 51) ~ "Ignorado",
    TRUE ~ "Reavaliar"
    )
  )


# salvando a primeira rodada de cnpj de mulheres
dbWriteTable(con, "aml_sexo", empresas_def_sex, overwrite = TRUE)
