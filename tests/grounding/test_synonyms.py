from nutrition_diary.grounding.synonyms import normalize_food_name


def test_normalize_strips_articles() -> None:
    assert normalize_food_name("The chicken") == "chicken"


def test_normalize_substitutes() -> None:
    assert "potato chips" in normalize_food_name("some chips")
    assert "eggplant" in normalize_food_name("aubergine curry")
