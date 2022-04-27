require(magrittr, include.only = "%>%")
require(dbplyr)

# Ler dados ---------------------------------------------------------------
url <- "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"

covid19 <- 
  read.csv(url, stringsAsFactors = FALSE)

covid19 %>% 
  dplyr::glimpse()

# Preparar dados ----------------------------------------------------------
covid19 <- 
  covid19 %>% 
  dplyr::filter(Country.Region %in% c("Brazil", "Peru", "Bolivia", 
                                      "Chile", "Argentina", "Colombia",
                                      "Venezuela", "Ecuador", "Uruguay",
                                      "Paraguay")) %>% 
  dplyr::select(- Province.State) %>% 
  tidyr::pivot_longer(!Country.Region:Long, 
                      names_to = "date", 
                      values_to = "cumulate") %>% 
  dplyr::group_by(Country.Region) %>% 
  dplyr::mutate(date = gsub("X", "", date) %>% 
                  gsub("\\.", "-", .) %>% 
                  lubridate::mdy(),
                value = cumulate - dplyr::lag(cumulate)) %>% 
  dplyr::rename(country = Country.Region)

# Salvar no DB2 -----------------------------------------------------------
readRenviron(".Renviron")

drv <-
  RJDBC::JDBC("com.ibm.db2.jcc.DB2Driver", "jars/db2jcc4.jar")

host <- Sys.getenv("DB2_HOST")
user <- Sys.getenv("DB2_USER")
password <- Sys.getenv("DB2_PASSWORD")

uri <- 
  sprintf("jdbc:db2://%s/bludb:user=%s;password=%s;sslConnection=true;", host, user, password)

db2 <-
  DBI::dbConnect(drv, uri)

DBI::dbWriteTable(db2, "COVID19", value = covid19, overwrite = TRUE)

# Ler dados do DB2 --------------------------------------------------------
dplyr::tbl(db2, "COVID19") %>%
  dplyr::filter(COUNTRY == "Brazil")

# Análises ----------------------------------------------------------------
brazil <- dplyr::tbl(db2, "COVID19") %>%
  dplyr::filter(COUNTRY == "Brazil") %>%
  dplyr::as_tibble() %>%
  dplyr::mutate(DATE = DATE %>% lubridate::ymd(),
                VALUE = ifelse(VALUE < 0, NA, VALUE))

## Distribuição do número de casos diários --------------------------------
brazil %>%
  ggplot2::ggplot() +
  ggplot2::aes(DATE, VALUE) +
  ggplot2::geom_line() +
  ggplot2::theme_minimal() +
  ggplot2::labs(x = "Data", 
                y = "# de casos de COVID 19")

## Distribuição da média móvel de 7 dias de casos -------------------------
brazil <- 
  brazil %>%
  dplyr::mutate(MOVING_AVERAGE = zoo::rollmean(VALUE, 7, align = "right", fill = NA))

brazil %>%
  ggplot2::ggplot() +
  ggplot2::aes(DATE, MOVING_AVERAGE) +
  ggplot2::geom_line() +
  ggplot2::theme_minimal() +
  ggplot2::labs(x = "Data", 
                y = "Média móvel de casos de COVID 19")

## Valor máximo da distribuição de média móvel de 7 dias de casos ---------
brazil %>%
  dplyr::filter(MOVING_AVERAGE == max(MOVING_AVERAGE, na.rm = TRUE)) %>%
  dplyr::select(COUNTRY, DATE, MOVING_AVERAGE) %>%
  reactable::reactable()

## Distribuição da média móvel de 7 dias de casos (plotly) ----------------
gg <- brazil %>%
  ggplot2::ggplot() +
  ggplot2::aes(DATE, MOVING_AVERAGE) +
  ggplot2::geom_line() +
  ggplot2::theme_minimal() +
  ggplot2::labs(x = "Data", 
                y = "Média móvel de casos de COVID 19")
gg %>% 
  plotly::ggplotly()

## Variação entre a média móvel mais atual e a média móvel anterior -------
dplyr::tbl(db2, "COVID19") %>%
  dplyr::distinct(DATE) %>%
  dplyr::arrange(desc(DATE))

delta <- dplyr::tbl(db2, "COVID19") %>%
  dplyr::filter(DATE %in% c("2022-04-22", "2022-04-23")) %>%
  dplyr::as_tibble() %>%
  dplyr::mutate(DATE = DATE %>% lubridate::ymd(),
                MOVING_AVERAGE = zoo::rollmean(VALUE, 7, align = "right", fill = NA)) %>%
  dplyr::group_by(COUNTRY) %>%
  dplyr::mutate(`DELTA%` = (MOVING_AVERAGE - dplyr::lag(MOVING_AVERAGE))/dplyr::lag(MOVING_AVERAGE) * 100,
                `DELTA%` = ifelse(is.na(`DELTA%`), 0, round(`DELTA%`, 1))) %>%
  dplyr::filter(DATE == "2022-04-23")

### Tabela
delta %>%
  dplyr::select(COUNTRY, `DELTA%`) %>%
  reactable::reactable(striped = TRUE, highlight = TRUE)

### Gráfico de barra
gg <- delta %>%
  dplyr::select(COUNTRY, `DELTA%`) %>%
  ggplot2::ggplot() +
  ggplot2::aes(COUNTRY, `DELTA%`) +
  ggplot2::geom_col(fill = ifelse(delta$`DELTA%` > 0, "#cb6e5d", "#005c5b")) +
  ggplot2::theme_minimal() +
  ggplot2::labs(x = "Países", y = "Delta %")
plotly::ggplotly(gg)

# Mapa com as variações percentuais ---------------------------------------
conpal <- 
  leaflet::colorNumeric(palette = "RdBu", 
                        domain = delta$`DELTA%`, 
                        na.color = "black", 
                        reverse = TRUE)

delta %>%
  dplyr::select(COUNTRY, LAT, LONG, `DELTA%`) %>%
  leaflet::leaflet() %>%
  leaflet::addProviderTiles("CartoDB.DarkMatter") %>%
  leaflet::addCircleMarkers(~LONG, ~LAT, 
                            label = paste(delta$COUNTRY, " ", 
                                          round(delta$`DELTA%`, 1)),
                            color = ~conpal(delta$`DELTA%`)) %>%
  leaflet::addLegend(position = "bottomleft", 
                     title = "Delta %",
                     pal = conpal, 
                     values = delta$`DELTA%`,
                     opacity = 0.8)

# Funções SQL pura --------------------------------------------------------
dplyr::tbl(db2, "COVID19") %>%
  dplyr::mutate(REF_DATE = dplyr::sql("VARCHAR_FORMAT(LAST_DAY(TO_DATE(DATE, 'YYYYMMDD')), 'YYYY-MM-DD')")) %>%
  dplyr::group_by(COUNTRY, REF_DATE) %>%
  dplyr::summarise(SUM = sum(VALUE, na.rm = TRUE), .groups = "drop") %>%
  dplyr::filter(COUNTRY == "Brazil")


  


