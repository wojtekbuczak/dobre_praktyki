import collections

## Funkcje Pythona

### 1. is_palindrome(text: str) -> bool
def is_palindrome(text: str) -> bool:
    """
    Sprawdza, czy dany ciąg znaków jest palindromem, ignorując wielkość liter i spacje.

    Args:
        text: Ciąg znaków do sprawdzenia.

    Returns:
        True, jeśli jest palindromem, False w przeciwnym razie.
    """
    # Usuwamy spacje i zamieniamy na małe litery
    processed_text = "".join(text.split()).lower()
    # Porównujemy z odwróconym ciągiem
    return processed_text == processed_text[::-1]

### 2. fibonacci(n: int) -> int
def fibonacci(n: int) -> int:
    """
    Zwraca n-ty element ciągu Fibonacciego.
    (fibonacci(0) == 0, fibonacci(1) == 1)

    Args:
        n: Indeks elementu ciągu (nieujemna liczba całkowita).

    Returns:
        n-ty element ciągu Fibonacciego.

    Raises:
        ValueError: Jeśli n jest ujemne.
    """
    if n < 0:
        raise ValueError("Indeks 'n' musi być nieujemny.")
    if n == 0:
        return 0
    if n == 1:
        return 1

    # Implementacja iteracyjna dla lepszej wydajności
    a, b = 0, 1
    for _ in range(2, n + 1):
        a, b = b, a + b
    return b

### 3. count_vowels(text: str) -> int
def count_vowels(text: str) -> int:
    """
    Zlicza liczbę samogłosek (a, e, i, o, u, y) w podanym ciągu, ignorując wielkość liter.

    Args:
        text: Ciąg znaków do przeanalizowania.

    Returns:
        Liczba samogłosek w tekście.
    """
    vowels = "aeiouy"
    text_lower = text.lower()
    count = 0
    for char in text_lower:
        if char in vowels:
            count += 1
    return count

### 4. calculate_discount(price: float, discount: float) -> float
def calculate_discount(price: float, discount: float) -> float:
    """
    Zwraca cenę po uwzględnieniu zniżki.

    Args:
        price: Pierwotna cena.
        discount: Współczynnik zniżki (musi być w zakresie [0, 1]).

    Returns:
        Cena po rabacie.

    Raises:
        ValueError: Jeśli discount jest spoza zakresu 0–1.
    """
    if not (0 <= discount <= 1):
        raise ValueError("Zniżka musi być w zakresie od 0 do 1 (włącznie).")

    final_price = price * (1 - discount)
    # Zapewnienie, że cena nie jest ujemna (choć powinno to być zablokowane przez ValueError)
    return max(0, final_price)

### 5. flatten_list(nested_list: list) -> list
def flatten_list(nested_list: list) -> list:
    """
    Przyjmuje listę (mogącą zawierać zagnieżdżone listy) i zwraca ją „spłaszczoną”.

    Args:
        nested_list: Lista zawierająca potencjalnie zagnieżdżone listy.

    Returns:
        Spłaszczona lista.
    """
    flat_list = []
    for element in nested_list:
        if isinstance(element, list):
            # Rekurencyjne wywołanie dla zagnieżdżonej listy
            flat_list.extend(flatten_list(element))
        else:
            # Dodanie pojedynczego elementu
            flat_list.append(element)
    return flat_list

### 6. word_frequencies(text: str) -> dict
def word_frequencies(text: str) -> dict:
    """
    Zwraca słownik z częstością występowania słów w tekście,
    ignorując wielkość liter i interpunkcję.

    Args:
        text: Ciąg znaków do analizy.

    Returns:
        Słownik, gdzie kluczem jest słowo (małe litery), a wartością jego częstość.
    """
    import re
    # Zamieniamy tekst na małe litery i usuwamy wszystkie znaki niebędące słowami (a-z, A-Z, 0-9, _)
    # a następnie zastępujemy je spacjami, aby słowa się oddzieliły.
    text = text.lower()
    cleaned_text = re.sub(r'[^\w\s]', '', text)

    # Dzielimy tekst na słowa (separatorem są spacje)
    words = cleaned_text.split()

    # Używamy collections.Counter do szybkiego zliczenia częstości
    return dict(collections.Counter(words))

### 7. is_prime(n: int) -> bool
def is_prime(n: int) -> bool:
    """
    Sprawdza, czy liczba całkowita jest liczbą pierwszą.
    Liczba pierwsza to liczba naturalna większa od 1, która ma
    dokładnie dwa dzielniki: jedynkę i siebie samą.

    Args:
        n: Liczba całkowita do sprawdzenia.

    Returns:
        True, jeśli liczba jest pierwsza, False w przeciwnym razie.
    """
    if n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False  # Wszystkie parzyste > 2 nie są pierwsze

    # Sprawdzamy nieparzyste dzielniki od 3 do pierwiastka z n
    i = 3
    while i * i <= n:
        if n % i == 0:
            return False
        i += 2  # Przechodzimy tylko przez nieparzyste liczby

    return True

# --- Przykładowe użycie ---

print("--- Testy Funkcji ---")
print(f"is_palindrome('Kajak'): {is_palindrome('Kajak')}")  # True
print(f"is_palindrome('A to krowa, ma kota'): {is_palindrome('A to krowa, ma kota')}") # False
print(f"fibonacci(6): {fibonacci(6)}")  # 8
print(f"count_vowels('Programowanie w Pythonie'): {count_vowels('Programowanie w Pythonie')}") # 9
print(f"calculate_discount(200, 0.25): {calculate_discount(200, 0.25)}")  # 150.0
try:
    calculate_discount(100, 1.1)
except ValueError as e:
    print(f"Błąd discount: {e}")
list_to_flatten = [1, [2, 3], [4, [5, 6, [7]]]]
print(f"flatten_list({list_to_flatten}): {flatten_list(list_to_flatten)}") # [1, 2, 3, 4, 5, 6, 7]
text_for_freq = "Ala ma kota, kot ma Ale i Ale zjada myszy."
print(f"word_frequencies('{text_for_freq}'): {word_frequencies(text_for_freq)}")
print(f"is_prime(17): {is_prime(17)}") # True
print(f"is_prime(15): {is_prime(15)}") # False