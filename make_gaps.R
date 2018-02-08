library(tidyverse)
library(broom)

data = read.table(text = '
                  dataset sample_id   observation estimate
                  A   A1  4.8 4.7
                  A   A2  4.3 4.5
                  A   A3  3.1 2.9
                  A   A4  2.1 2
                  A   A5  1.1 1
                  B   B1  4.5 4.3
                  B   B2  3.9 4.1
                  B   B3  2.9 3
                  B   B4  1.8 2
                  B   B5  1   1.2
                  ', header = TRUE)


data %>%
  group_by(dataset) %>% 
  nest() %>% 
  mutate(mod = map(.x = data, ~lm(observation ~ estimate, data = .x)),
         aug = map(mod, data, ~augment_columns(.x, .y))) %>% 
  unnest(aug)

lm(observation ~ estimate, data = .)