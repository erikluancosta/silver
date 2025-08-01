library(dplyr)
library(genderBR)
library(stringi)
library(stringr)
source("R/conectar.R")
nomes_empresas <- conectar("receitaf")

#-----------------------------------
# Montando a query de empresas
#-----------------------------------
empresa_endereco <- dbGetQuery(
  nomes_empresas,
  "
  SELECT 
    /* monta o CNPJ completo já no SELECT */
    (es.cnpj_basico || es.cnpj_ordem || es.cnpj_dv) AS cnpj_completo,
    es.cnpj_basico,
    es.cnpj_ordem,
    es.cnpj_dv,
    es.nome_fantasia,
    --em.razao_social,
    es.identificador_matriz_filial,
    es.situacao_cadastral,
    es.tipo_logradouro,
    es.logradouro,
    es.numero,
    es.complemento,
    es.bairro,
    es.cep,
    es.uf,
    --es.municipio,
    mu.descricao         AS municipio_descricao,
    mu.cd_uf,
    mu.cd_mun_7 as municipio
  FROM estabelecimento es
  LEFT JOIN munic_comp mu
    ON es.municipio = mu.codigo
  LEFT JOIN empresa em
    ON es.cnpj_basico = em.cnpj_basico
  WHERE es.situacao_cadastral = 2
    AND es.uf IN ('AC','AM','AP','PA','RO','RR','TO', 'MT', 'MA')
    --AND em.natureza_juridica in (
    --  2151, 2291, 2143, 2305, 2011, 2135, 2178, 2062,
		--	2160, 2046, 2054, 2038, 2127, 2097, 2089, 2070
		--);
  "
)

empresa_endereco <- empresa_endereco |> 
  mutate(
    numero_final = if_else(
      numero %in% c("", "S/N", "SN", "0", "SN.", "SNR", "Sn", "sn", "S/N.",
                    "S/NR", "s/n", "S/A", "S.N", "S N", "S / N", "S-N", "s-n",
                    "SEM NM", "SEM NM?", "SEM N?", "s n"),
      NA_character_,
      numero
    )
  ) |> 
  # Cria logradouro_final juntando tipo_logradouro e logradouro
  mutate(
    logradouro_final = str_squish(
      paste(
        tipo_logradouro,
        if_else(is.na(logradouro), "", logradouro)
      )
    )
  ) |> 
  # Cria coluna apenas com os números do campo "numero"
  mutate(
    numero_final = str_extract_all(numero_final, "\\d+") |> 
      sapply(paste0, collapse = "") |> 
      as.character()
  ) |> 
  # Seleciona colunas finais
  #select(
  #  cnpj_reconstruido, nome_fantasia, razao_social, logradouro_final, numero, 
  #  numero_final, cep, bairro
  #) |> 
  # Adiciona coluna fonte
  mutate(fonte = "cnpj") |> 
  vitallinkage::as_char()


#-----------------------------------
# Montando a query de empresas
#-----------------------------------

enderecos_cnefe <- dbGetQuery(
  nomes_empresas,
  'SELECT "COD_UNICO_ENDERECO" as id_endereco, 
          "COD_UF" as cd_uf,
          mc.descricao as municipio_descricao,
          "COD_MUNICIPIO" as municipio,
          "CEP" as cep,
          "DSC_LOCALIDADE" as bairro,
          "NOM_TIPO_SEGLOGR" as tipo_logradouro,
          "NOM_TITULO_SEGLOGR",
          "NOM_SEGLOGR",
          "NUM_ENDERECO" as numero,
          "VAL_COMP_ELEM1",
          "DSC_MODIFICADOR",
          "LATITUDE",
          "LONGITUDE",
          "DSC_ESTABELECIMENTO" as nome_fantasia
  FROM enderecos_cnefe
  LEFT JOIN
          munic_comp as mc ON enderecos_cnefe."COD_MUNICIPIO" = mc.cd_mun_7;'
)

enderecos_cnefe <- enderecos_cnefe |> 
  mutate(
    NOM_TITULO_SEGLOGR = ifelse(NOM_TITULO_SEGLOGR == "", NA, NOM_TITULO_SEGLOGR),
    logradouro_final = paste(
      tipo_logradouro,
      ifelse(is.na(NOM_TITULO_SEGLOGR), "", NOM_TITULO_SEGLOGR),
      NOM_SEGLOGR
    ) |> 
      str_squish(),  # Remove espaços extras, incluindo duplos espaços entre as palavras
      numero_final = str_extract_all(numero, "\\d+")
    ) |> 
  #select(logradouro_final, NUM_ENDERECO, VAL_COMP_ELEM1, CEP, DSC_ESTABELECIMENTO, DSC_LOCALIDADE, LATITUDE, LONGITUDE,
  #) |> 
  rename(
    "complemento_numerico"=VAL_COMP_ELEM1,
  ) |> 
  mutate(fonte= "cnefe",
         complemento_numerico = ifelse(complemento_numerico == "", NA, complemento_numerico)) |> 
  vitallinkage::as_char() |> 
  select(
    -NOM_TITULO_SEGLOGR,
    -NOM_SEGLOGR
  )


#-----------------------------------
# Montando base de linkage de endereços
#-----------------------------------
base_final_linkage <- bind_rows(empresa_endereco, enderecos_cnefe)

rm(empresa_endereco, enderecos_cnefe)

base_final_linkage <- base_final_linkage |>
  mutate(cep5 = substr(as.character(cep), 1, 5))

# LINKAGE
source("R/algoritmo.R")

# Rodada 1
base_final_linkage <- base_final_linkage |> 
  start_linkage2(c("logradouro_final", 'cep', 'numero_final', 'bairro', 'municipio', 'cd_uf'))

base_final_linkage <- base_final_linkage |> 
  sample_coords(round = 1)

# Rodada 2
base_final_linkage <- base_final_linkage |> 
  regras_linkage_dt(c('cep', 'numero_final', 'bairro', 'municipio', 'cd_uf'), 2)

base_final_linkage <- base_final_linkage |> 
  sample_coords(round = 2)

# Rodada 3
base_final_linkage <- base_final_linkage |> 
  regras_linkage_dt(c('nome_fantasia', 'numero', 'bairro', 'municipio', 'cd_uf'), 3)

base_final_linkage <- base_final_linkage |> 
  sample_coords(round = 3)

# Rodada 4
base_final_linkage <- base_final_linkage |> 
  regras_linkage_dt(c("logradouro_final", 'numero_final', 'bairro', 'municipio', 'cd_uf'), 4)

base_final_linkage <- base_final_linkage |> 
  sample_coords(round = 4)

# Rodada 5
base_final_linkage <- base_final_linkage |> 
  regras_linkage_dt(c("logradouro_final", 'cep5', 'numero_final', 'bairro', 'municipio', 'cd_uf'), 5)

base_final_linkage <- base_final_linkage |> 
  sample_coords(round = 5)

# Rodada 6
base_final_linkage <- base_final_linkage |> 
  regras_linkage_dt(c("logradouro_final", 'cep', 'numero_final', 'municipio', 'cd_uf'), 6)

base_final_linkage <- base_final_linkage |> 
  sample_coords(round = 6)

# Rodada 7
base_final_linkage <- base_final_linkage |> 
  regras_linkage_dt(c("logradouro_final", 'cep5', 'numero_final', 'municipio', 'cd_uf'), 7)

base_final_linkage <- base_final_linkage |> 
  sample_coords(round = 7)
