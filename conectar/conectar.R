library(DBI)
library(RPostgres)

conectar <- function(banco = c("receitaf", "testes")) {
  banco <- match.arg(banco)
  
  con <- dbConnect(
    Postgres(),
    host = Sys.getenv(paste0("DB_HOST_", toupper(banco))),
    port = as.integer(Sys.getenv(paste0("DB_PORT_", toupper(banco)))),
    user = Sys.getenv(paste0("DB_USER_", toupper(banco))),
    password = Sys.getenv(paste0("DB_PASSWORD_", toupper(banco))),
    dbname = Sys.getenv(paste0("DB_NAME_", toupper(banco)))
  )
  
  if (dbIsValid(con)) {
    message("Conexão com o banco '", banco, "' estabelecida com sucesso!")
  } else {
    message("Falha na conexão com o banco '", banco, "'.")
  }
  
  return(con)
}

con <- conectar('receitaf')

rm(conectar)