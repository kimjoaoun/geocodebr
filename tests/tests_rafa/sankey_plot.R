
############## sankey plot ----------------------------------------
library(ggplot2)
library(ggsankey)
library(dplyr)
library(data.table)

system.time(danid <- dani()) # 60.10
system.time(rafad <- rafa()) # 39.54


df <- left_join(select(danid, c('id', 'match_type')),
                select(rafad, c('id', 'match_type')), by='id')

data.table::setDT(df)
data.table::setnames(
  df,
  old = c('match_type.x', 'match_type.y'),
  new = c('case_dani', 'case_rafa') )


transition_mtrx <- round(table(df$case_dani, df$case_rafa) / nrow(df)*100, 1)


df[, case_dani := as.integer(gsub('case_', '', case_dani)) ]
df_count <- df[, .(count = round(.N / nrow(df)*100, 2)), by= .(case_dani, case_rafa)]

# df_long <- data.table::melt(data = df, id.vars='id')
# head(df_long)

# df_sankey <- ggsankey::make_long(df, case_dani, case_rafa)
df_sankey <- ggsankey::make_long(df_count, case_dani, case_rafa, value = count)
df_sankey$value <- round(df_sankey$value / nrow(df)*100, 2)
head(df_sankey)

#cats <- c(1:4, 44, 5:12)
cats <- c(12:5, 44, 4:1)
df_sankey$node <- factor(x = df_sankey$node,
                         levels = cats,
                         ordered = T)

df_sankey$next_node <- factor(x = df_sankey$next_node,
                              levels = cats,
                              ordered = T)

fig <- ggplot(data = df_sankey,
              aes(x = x,
                  next_x = next_x,
                  node = node,
                  next_node = next_node,
                  fill = factor(node)
                  , label = value
              )) +
  geom_sankey() +
  theme_sankey(base_size = 16)  + geom_sankey_label()

fig
plotly::ggplotly(fig)


library(plotly)

fig <- plot_ly(
  type = "sankey",
  orientation = "h",

  node = list(
    label = c("A1", "A2", "B1", "B2", "C1", "C2"),
    color = c("blue", "blue", "blue", "blue", "blue", "blue"),
    pad = 15,
    thickness = 20,
    line = list(
      color = "black",
      width = 0.5
    )
  ),

  link = list(
    source = c(0,1,0,2,3,3),
    target = c(2,3,3,4,4,5),
    value =  c(8,4,2,8,4,2)
  )
)
fig <- fig %>% layout(
  title = "Basic Sankey Diagram",
  font = list(
    size = 10
  )
)

fig





df_count <- df[, .(count = round(.N / nrow(df)*100, 2)), by= .(case_dani, case_rafa)]

df_sankey <- ggsankey::make_long(df, case_dani, case_rafa)

df_sankey <- left_join(df_sankey, df_count,
          by=c('node'='case_dani', 'next_node' = 'case_rafa'))


head(df_sankey)

#cats <- c(1:4, 44, 5:12)
cats <- c(12:5, 44, 4:1)
df_sankey$node <- factor(x = df_sankey$node,
                         levels = cats,
                         ordered = T)

df_sankey$next_node <- factor(x = df_sankey$next_node,
                              levels = cats,
                              ordered = T)
data.table::setDT(df_sankey)[, count2 := ifelse(is.na(count), lead(count), count)]
head(df_sankey)

fig <- ggplot(data = df_sankey,
              aes(x = x,
                  next_x = next_x,
                  node = node,
                  next_node = next_node,
                  fill = factor(node)
                  , label = count2
              )) +
  geom_sankey() +
  theme_sankey(base_size = 16)  + geom_sankey_label()

fig
plotly::ggplotly(fig)
