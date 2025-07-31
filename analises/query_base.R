library(dplyr)
source('conectar/conectar.R')

silver <- dbGetQuery(
  con,
  "
  WITH est_filtrado AS (
  SELECT 
    cnpj_basico, cnpj_ordem, cnpj_dv, nome_fantasia, situacao_cadastral,
    data_situacao_cadastral, data_inicio_atividade,
    cnae_fiscal_principal, identificador_matriz_filial,
    tipo_logradouro, logradouro, numero, complemento,
    bairro, cep, uf, municipio, ddd_1, telefone_1, correio_eletronico
  FROM estabelecimento
  WHERE situacao_cadastral = 2
    AND uf IN ('MS', 'TO', 'GO', 'BA', 'PA', 'MA', 'PR', 'CE', 'MT', 'SP', 'PI')
    ),
    sexo_agg AS (
      SELECT 
        cnpj_basico,
        COUNT(*) FILTER (WHERE sexo = 'Masc') AS sx_masculino,
        COUNT(*) FILTER (WHERE sexo = 'Fem') AS sx_feminino,
        COUNT(*) FILTER (WHERE sexo IS NULL OR sexo NOT IN ('Masc', 'Fem')) AS sx_ign
      FROM socios_nomes
      WHERE identificador_socio = 2
      GROUP BY cnpj_basico
    )
    SELECT 
      (es.cnpj_basico || es.cnpj_ordem || es.cnpj_dv) AS cnpj_completo,
      es.*,  -- já otimizado no CTE
      em.razao_social,
      em.natureza_juridica,
      n.descricao AS ds_natureza_juridica,
     -- c.codigo AS cnae_fiscal_principal,
      c.descricao AS ds_cnae_fiscal_principal,
      c.cnse2_denom AS cnae_denom,
      c.cngr2_denom AS cnae_grupo,
      c.cndv2_denom,
      em.porte_empresa,
      em.capital_social,
      mu.descricao AS municipio_descricao,
      mu.cd_mun_7 AS cd_ibge7,
      eg.sexo AS sexo_mei,
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
    )
    AND mu.cd_mun_7 IN (
      5000203, 1700301, 1700400, 5201108, 2901403, 1701903, 1702109, 1702158, 1702208, 1702554,
      1702901, 1703008, 2903201, 2903904, 5002308, 1703701, 1703800, 5002704, 1703842, 1502152,
      1703867, 2102804, 1705102, 1704600, 1705508, 1716703, 5003207, 1707009, 1707108, 1707405,
      2104057, 2911105, 1708205, 1708254, 5208707, 1708304, 1709302, 1709500, 2913200, 5004304,
      1710904, 5004908, 5005004, 4113700, 2919553, 1712504, 1712702, 1713304, 1713601, 1713700,
      1714203, 1714880, 2309300, 1505494, 1721000, 1716109, 1716208, 2923704, 1505536, 1716505,
      1716604, 1717008, 1717503, 1717909, 1718006, 2109007, 5006903, 1718204, 5218300, 1718303,
      1718402, 5107040, 2109502, 2926202, 5007109, 3543402, 1506195, 2927408, 1718907, 1719004,
      1720101, 2928901, 1720150, 1507300, 1507458, 2111300, 5007901, 1720655, 5107909, 1720804,
      1720903, 5008008, 2211001, 1721208, 5008305, 1721307, 1722081
    );
  "
)

silver_sx <- silver |> 
  mutate(
    rating_sexo_masc = sx_masculino / (sx_masculino + sx_feminino + sx_ign)*100,
    rating_sexo_fem = sx_feminino / (sx_masculino + sx_feminino + sx_ign)*100
  )



silver_sx <- silver_sx |>
  mutate(
    sexo_final = case_when(
      (rating_sexo_fem >=51 | sexo_mei == "Fem") ~ "Fem",
      (rating_sexo_masc >=51 | sexo_mei == "Masc") ~ "Masc",
      (rating_sexo_masc == 50 & rating_sexo_fem == 50) ~ "Ambos",
      (is.na(sexo_mei) & rating_sexo_fem < 51 & rating_sexo_masc < 51) ~ "Ignorado",
      TRUE ~ "Reavaliar"
    )
  )

