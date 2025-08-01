library(dplyr)

final <- base_final_linkage |> 
  filter(
    fonte == "cnpj"
  ) |> 
  mutate(
    cnpj_basico=as.numeric(cnpj_basico),
    cnpj_ordem=as.numeric(cnpj_ordem),
    cnpj_dv=as.numeric(cnpj_dv),
    cep=as.numeric(cep),
    latitude_final = coalesce(temp_latitude1, temp_latitude2, temp_latitude3, temp_latitude4, temp_latitude5, temp_latitude6, temp_latitude7),
    longitude_final = coalesce(temp_longitude1, temp_longitude2, temp_longitude3, temp_longitude4, temp_longitude5, temp_longitude6, temp_longitude7),
    base_origem = coalesce(geo_fonte1, geo_fonte2, geo_fonte3, geo_fonte4, geo_fonte5, geo_fonte6, geo_fonte7)
  ) |> 
  select(cnpj_basico,
         cnpj_ordem,
         cnpj_dv,
         nome_fantasia,
         identificador_matriz_filial,
         situacao_cadastral,
         tipo_logradouro,
         logradouro,
         numero,
         complemento,
         bairro,
         cep,
         uf,
         municipio_descricao,
         cd_uf,
         municipio,
         latitude_final,
         longitude_final,
         base_origem
  )

#-------------------------
# Endereços do Marcelo
#-------------------------
marcelo <- read.csv2("data/CNPJsGeocodificadosAteNivelCEP8/CNPJsGeocodificadosAteNivelCEP8.csv")

marcelo <- marcelo |> 
  mutate(
    complemento = ifelse(complemento == "", NA, complemento),
    marcelo = "marcelo"
  )

# Join com os endereços do Marcelo
final <- final |> 
  left_join(
    marcelo, by = c(
      "cnpj_basico" = "cnpj",
      "cnpj_ordem" = "compl", 
      "cnpj_dv" = "dv",
      "tipo_logradouro" = "tipolog",
      "logradouro" = "logradouro",
      "numero" = "numero",
      "bairro" = "bairro",
      "complemento"="complemento",
      "cep" = "cep",
      "uf" = "uf"
    )
  ) |> 
  mutate(
    lat = as.numeric(lat),
    lng = as.numeric(lng),
    base_origem2 = coalesce(base_origem, marcelo),
    latitude = coalesce(latitude_final, lat),
    longitude = coalesce(longitude_final, lng),
    cnpj_basico = stringr::str_pad(cnpj_basico, width = 8, side = "left", pad = "0"),
    cnpj_ordem = stringr::str_pad(cnpj_ordem, width = 4, side = "left", pad = "0"),
    cnpj_dv = stringr::str_pad(cnpj_dv, width = 2, side = "left", pad = "0")
  )

library(sf)
final$latitude <- as.numeric(final$latitude)
final$longitude <- as.numeric(final$longitude)

final_geo <- final |> 
  filter(!is.na(latitude), !is.na(longitude)) |> 
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326) # WGS 84

st_write(final_geo, dsn = con, layer = "cnpj_geocodificados2", append = FALSE)


#-------------------------
# Mandando para o servidor
#-------------------------
con <- conectar("receitaf")
dbWriteTable(con, "cnpj_geocodificados2", final, overwrite = TRUE, row.names = FALSE)




#-------------------------
# ACOMPANHAMENTO
#-------------------------
final |> filter(em.natureza_juridica == 1) |> vitaltable::tab_1(base_origem2)

sfiltrar <- final |>
  filter(!is.na(geo_fonte2)) |> 
  pull(par_1)


a <- base_final_linkage |> 
  filter(
    fonte == "cnpj" &
      !is.na(par_c7) & 
      !is.na(geo_fonte7) &
      !is.na(temp_latitude7) &
      is.na(temp_latitude1) &
      is.na(temp_latitude2) &
      is.na(temp_latitude3) &
      is.na(temp_latitude4) &
      is.na(temp_latitude5) &
      is.na(temp_latitude6)
    
  )

b <- base_final_linkage |>
  filter(par_1 %in% a$par_1) |> 
  select(-original_index, -par_2, -cd_uf, -tipo_logradouro)
