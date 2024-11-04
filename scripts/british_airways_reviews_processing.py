import pandas as pd
import re
import os
from loguru import logger
import spacy
from spacy.lang.en.stop_words import STOP_WORDS
from spacy.tokens import Doc
from spellchecker import SpellChecker

logger.add(
    "../logs/data_processing.log",
    rotation="5 MB",
    retention="10 days",
    level="INFO",
    enqueue=True,
    backtrace=True,
    diagnose=True
)

nlp = spacy.load('en_core_web_sm')

# Extend stopwords with domain-specific words
custom_stopwords = set(['airline', 'flight', 'british', 'airways', 'ba', 'plane'])
STOP_WORDS |= custom_stopwords

# Initialize spell checker and translator
spell = SpellChecker()

df = pd.read_csv("../data/raw/british_airways_raw_reviews.csv")
logger.info(f"Loaded raw data with {len(df)} reviews.")
