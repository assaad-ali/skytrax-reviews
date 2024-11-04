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

def clean_text(text):
    text = re.sub(r"(✅\s*Trip\s*Verified\s*\|\s*|Not\s*Verified\s*\|\s*)", "", text, flags=re.IGNORECASE)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def correct_spelling(text):
    corrected_text = []
    for word in text.split():
        corrected_word = spell.correction(word)
        corrected_text.append(corrected_word if corrected_word else word)
    return ' '.join(corrected_text)

def process_text(text):
    try:
        # Spell correction
        text = correct_spelling(text)

        # NLP processing
        doc = nlp(text.lower())

        # Lemmatization, stopword removal, and handling negations
        tokens = []
        for token in doc:
            # Check if the token is not a stopword and not a punctuation
            if not token.is_stop and not token.is_punct and token.lemma_ not in STOP_WORDS:
                if token.dep_ == 'neg':
                    tokens.append('not_' + token.head.lemma_)
                else:
                    tokens.append(token.lemma_)

        processed_text = ' '.join(tokens)

        # Named Entity Recognition
        entities = [(ent.text, ent.label_) for ent in doc.ents]

        return processed_text, tokens, entities
    except Exception as e:
        logger.error(f"Error processing text: {e}")
        return "", [], []


