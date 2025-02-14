import math
import json
import multiprocessing
import os
from tqdm.contrib.concurrent import process_map
from rakun2 import RakunKeyphraseDetector

def process_chunk(params):
    """
    Processes a chunk of reviews.

    Parameters:
      params (tuple): (start_idx, chunk)
          - start_idx: the global starting index for this chunk
          - chunk: a list of reviews

    Returns:
      List of tuples (global_index, review_id, keywords)
    """
    start_idx, chunk = params
    detector = RakunKeyphraseDetector()
    results = []
    for i, review in enumerate(chunk):
        global_idx = start_idx + i
        text = review["text"]
        # Skip if text is empty or too short (fewer than 30 words)
        if not text.strip() or len(text.split()) < 30:
            results.append((global_idx, review["review_id"], []))
            continue

        keywords_with_scores = detector.find_keywords(text, input_type="string")
        # If detector returns keyword-score pairs, extract only the keywords.
        if keywords_with_scores and isinstance(keywords_with_scores[0], (list, tuple)):
            keywords = [kw for kw, _ in keywords_with_scores]
        else:
            keywords = keywords_with_scores
        results.append((global_idx, review["review_id"], keywords))
    return results

if __name__ == "__main__":
    # For Windows support (if needed)
    multiprocessing.freeze_support()

    # Set num_reviews for testing (0 = process all)
    num_reviews = 0

    # Read reviews from a JSON file
    with open(os.environ.get("DATA_PATH"), "r", encoding="utf-8") as infile:
        reviews = json.load(infile)
    print("File opened.")

    # Process only a subset if requested
    if num_reviews > 0:
        reviews = reviews[:num_reviews]

    # Determine the number of CPU cores and split data into that many chunks
    num_cores = multiprocessing.cpu_count()
    print(f"Detected {num_cores} CPU cores. Splitting data into {num_cores} chunks...")

    chunk_size = math.ceil(len(reviews) / num_cores)
    chunks = [reviews[i : i + chunk_size] for i in range(0, len(reviews), chunk_size)]

    # Prepare parameters: each element is (start_idx, chunk)
    params = []
    start_idx = 0
    for chunk in chunks:
        params.append((start_idx, chunk))
        start_idx += len(chunk)

    # Process each chunk in parallel using process_map.
    # The progress bar here represents chunks processed.
    chunk_results_list = process_map(process_chunk, params, max_workers=num_cores)

    # Reassemble results into a list with the same order as the original reviews.
    final_results = [None] * len(reviews)
    for chunk_results in chunk_results_list:
        for global_idx, review_id, keywords in chunk_results:
            final_results[global_idx] = {"review_id": review_id, "keywords": keywords}

    # Write the results to an output JSON file
    with open("output.json", "w", encoding="utf-8") as outfile:
        json.dump(final_results, outfile, indent=2)

    print("Keyphrase extraction complete. Results saved to output.json.")