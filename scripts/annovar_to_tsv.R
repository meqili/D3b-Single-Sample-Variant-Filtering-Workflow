library(tidyverse)
library(tidyr)
library(vcfR)

readVcf <- function(vcf_file) {
  vcf <- read.vcfR(vcf_file)
  v <- vcfR2tidy(vcf)
  csqd <- v$meta %>% filter(ID == "CSQ") %>% select(Description)
  csqd <- unlist(str_split(gsub("Consequence annotations from Ensembl VEP. Format: ", "", csqd$Description), "\\|"))
  annd <- v$meta %>% filter(ID == "ANN") %>% select(Description)
  annd <- unlist(str_split(gsub("'", "", gsub(" ","" ,gsub("Functional annotations: ", "", annd$Description))), "\\|"))
  
  if(!is.null(csqd)){
    CSQ <- v$fix %>%  
      separate_rows(CSQ, sep = ",") %>% 
      separate(CSQ, csqd, sep = "\\|", convert = TRUE) %>%
      filter(str_detect(Feature, "ENST")) %>%
      unite("VariantID", c(CHROM, POS, REF, ALT), sep = "-", remove = FALSE) %>% 
      unite("key", ChromKey, POS, sep = "-", remove = FALSE) 
  }
  if(!is.null(annd)){
    ANN <- 	v$fix %>%  
      unite("VariantID", c(CHROM, POS, REF, ALT), sep = "-", remove = FALSE) %>% 
      select("VariantID", "ANN") %>%
      separate_rows(ANN, sep = ",") %>%
      separate(ANN, annd, sep = "\\|", convert = TRUE) %>%
      filter(str_detect(Feature_ID, "ENST")) %>%
      mutate(Feature_ID = str_extract(as.character(Feature_ID), "ENST[0-9]+"))
  }
  
  if(is.null(csqd)){
    vf <- ANN
    } else{
    vf <- CSQ
    }

  gt <- v$gt %>% unite("key", ChromKey, POS, sep = "-", remove = FALSE)
  variants <- vf %>% left_join(gt, by = "key") %>% select(Indiv, everything()) %>% select(-contains(".y"))
  variants <- variants %>% rename(chromosome = CHROM, start = POS.x, reference = REF,
                                  alternate = ALT, quality = QUAL) %>% 
    select(-key, -ChromKey.x, -VariantID) %>%
    mutate_all(~ str_replace_all(., "\\\\x3b", ";")) %>%
    mutate_all(~ str_replace_all(., "\\\\x3d", "="))
  
  return(variants)
}

args <- commandArgs(trailingOnly = TRUE)
variants_list <- readVcf(args[1])
file_name <- args[2]

variants_list %>% write.table(file = paste0(file_name, ".tsv"),
              quote = FALSE, row.names = FALSE, col.names = TRUE, sep = "\t")