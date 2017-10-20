package apoc.number;

public enum RomanNumerals {
    I("I", 1),
    V("V", 5),
    X("X", 10),
    L("L", 50),
    C("C", 100),
    D("D", 500),
    M("M", 1000);

    private final String symbol;
    private final int value;

    RomanNumerals(final String symbol, final int value) {
        this.symbol = symbol;
        this.value = value;
    }

    String symbol() {
        return symbol;
    }

    int value() {
        return value;
    }

    public RomanNumerals getMultipleOfFive() {
        return this.ordinal() < RomanNumerals.values().length - 1
                ? RomanNumerals.values()[this.ordinal() + 1] : null;
    }

    public RomanNumerals getMultipleOfTen() {
        return this.ordinal() < RomanNumerals.values().length - 1
                ? RomanNumerals.values()[this.ordinal() + 2] : null;
    }
}