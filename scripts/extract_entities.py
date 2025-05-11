import sys
import json
import logging
import os
from gliner import GLiNER

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_gliner_model() -> GLiNER:
    """
    Load a pre-trained GLiNER model. If it hasn't been downloaded yet, it
    will be downloaded from the Hugging Face Model Hub automatically.

    Returns:
    An instance of the pre-trained GLiNER model.
    """
    try:
        logger.info("Loading GLiNER model...")
        model = GLiNER.from_pretrained("urchade/gliner_multi-v2.1")
        logger.info("GLiNER model loaded successfully")
        return model
    except Exception as e:
        logger.error(f"Failed to load GLiNER model: {str(e)}")
        raise

def extract_entities(
    model: GLiNER, text: str, labels: list[str] | None = None, threshold: float = 0.5
) -> list[dict]:
    """
    Extract entities from a given text using a GLiNER model.
    
    Args:
        model: The GLiNER model to be used for entity extraction.
        text: The text from which to extract entities.
        labels (optional): The entity types to be extracted, in lower case or title case.
        threshold (optional): The minimum confidence score required for an entity to be returned.
        
    Returns:
        A list of dictionaries, each containing the text and start and end indices of an extracted entity.
    """
    if labels is None:
        labels = ["Person", "Company", "Location"]
    try:
        logger.info(f"Extracting entities with labels: {labels}")
        entities = model.predict_entities(text, labels, threshold=threshold)
        logger.info(f"Successfully extracted {len(entities)} entities")
        return entities
    except Exception as e:
        logger.error(f"Entity extraction failed: {str(e)}")
        raise

def main():
    """Main function to process input text and output entities."""
    try:
        if len(sys.argv) != 3:
            logger.error("Usage: python extract_entities.py <input_file> <output_file>")
            sys.exit(1)

        input_file = sys.argv[1]
        output_file = sys.argv[2]
        logger.info(f"Reading input file: {input_file}")
        
        if not os.path.exists(input_file):
            logger.error(f"Input file not found: {input_file}")
            sys.exit(1)

        with open(input_file, 'r', encoding='utf-8') as f:
            text = f.read()
        logger.info(f"Read {len(text)} characters from input file")

        model = load_gliner_model()
        entities = extract_entities(model, text)
        
        # Write entities to output JSON file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(entities, f, ensure_ascii=False, indent=4)
        logger.info(f"Entities written to {output_file}")
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()