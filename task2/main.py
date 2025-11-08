import string
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import requests
import matplotlib.pyplot as plt


# --- Text download ---
def get_text(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Error fetching URL: {e}")
        return None


# --- MapReduce helpers ---
def remove_punctuation(text):
    return text.translate(str.maketrans("", "", string.punctuation))


def map_function(word):
    return word.lower(), 1


def shuffle_function(mapped_values):
    shuffled = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()


def reduce_function(key_values):
    key, values = key_values
    return key, sum(values)


def map_reduce(text, search_words=None):
    text = remove_punctuation(text)
    words = text.split()

    # Optionally filter words
    if search_words:
        words = [word for word in words if word in search_words]

    # --- Map step (parallel) ---
    with ThreadPoolExecutor() as executor:
        mapped_values = list(executor.map(map_function, words))

    # --- Shuffle step ---
    shuffled_values = shuffle_function(mapped_values)

    # --- Reduce step (parallel) ---
    with ThreadPoolExecutor() as executor:
        reduced_values = list(executor.map(reduce_function, shuffled_values))

    return dict(reduced_values)


# --- Visualization ---
def visualize_top_words(counter_dict, top_n=15):
    # Get top N words
    sorted_items = sorted(counter_dict.items(), key=lambda x: x[1], reverse=True)[:top_n]
    if not sorted_items:
        print("No words to display.")
        return

    words, counts = zip(*sorted_items)
    plt.figure(figsize=(10, 6))
    plt.barh(words[::-1], counts[::-1], color='skyblue')
    plt.xlabel('Frequency')
    plt.title(f'Top {top_n} Words')
    plt.show()


# --- Main function ---
def main():
    url = "https://www.gutenberg.org/files/11/11-0.txt"  # Example URL
    text = get_text(url)
    if not text:
        print("Failed to get text from URL.")
        return

    result = map_reduce(text)
    print("Top words frequency:", dict(sorted(result.items(), key=lambda x: x[1], reverse=True)[:15]))

    visualize_top_words(result, top_n=15)


if __name__ == "__main__":
    main()