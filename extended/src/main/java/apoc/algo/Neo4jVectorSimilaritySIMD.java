package apoc.algo;

import jdk.incubator.vector.FloatVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

// TODO - compare..
//  servirebbe una cosa di questa, ma non posso farlo.. 
public class Neo4jVectorSimilaritySIMD {

    // Seleziona la "forma" del vettore SIMD più grande disponibile sulla CPU, fino a 256 bit.
    // Questo caricherà 8 float (8 * 32bit = 256bit) alla volta.
    private static final VectorSpecies<Float> SPECIES = FloatVector.SPECIES_256;

    /**
     * Calcola la similarità per vettori FLOAT32 usando la Java Vector API per l'accelerazione SIMD.
     */
    private double calculateFloat32_SIMD(float[] v1, float[] v2) {
        // Inizializza i vettori accumulatori a zero. Questi conterranno somme parziali.
        FloatVector dotProductVec = FloatVector.zero(SPECIES);
        FloatVector normAVec = FloatVector.zero(SPECIES);
        FloatVector normBVec = FloatVector.zero(SPECIES);

        // Calcola il limite superiore per il ciclo vettoriale.
        // Assicura che processiamo solo blocchi completi.
        int loopBound = SPECIES.loopBound(v1.length);

        // --- CICLO VETTORIALE (SIMD) ---
        // Processa i dati in blocchi della dimensione di SPECIES (es. 8 elementi alla volta).
        for (int i = 0; i < loopBound; i += SPECIES.length()) {
            // Carica un blocco di dati dagli array Java nei vettori SIMD
            FloatVector va = FloatVector.fromArray(SPECIES, v1, i);
            FloatVector vb = FloatVector.fromArray(SPECIES, v2, i);

            // Calcola il prodotto scalare parziale usando FMA (Fused Multiply-Add: a * b + c)
            // È più efficiente di una moltiplicazione seguita da un'addizione.
            dotProductVec = va.fma(vb, dotProductVec);

            // Calcola le norme parziali
            normAVec = va.fma(va, normAVec); // va * va + normAVec
            normBVec = vb.fma(vb, normBVec); // vb * vb + normBVec
        }

        // "Riduci" i vettori accumulatori a un singolo valore scalare (double)
        // Sommando tutte le "lane" (corsie) del vettore SIMD.
        double dotProduct = dotProductVec.reduceLanes(VectorOperators.ADD);
        double normA = normAVec.reduceLanes(VectorOperators.ADD);
        double normB = normBVec.reduceLanes(VectorOperators.ADD);

        // --- CICLO SCALARE (per la "coda") ---
        // Processa gli elementi rimanenti che non rientravano in un blocco completo.
        for (int i = loopBound; i < v1.length; i++) {
            dotProduct += v1[i] * v2[i];
            normA += v1[i] * v1[i];
            normB += v2[i] * v2[i];
        }

        if (normA == 0.0 || normB == 0.0) {
            return 0.0;
        }

        return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    // Il metodo pubblico esterno applicherebbe poi la normalizzazione
    public double cosineSimilarity(float[] v1, float[] v2) {
        if (v1.length != v2.length) {
            throw new IllegalArgumentException("I vettori devono avere la stessa dimensione");
        }
        double rawSimilarity = calculateFloat32_SIMD(v1, v2);
        return (rawSimilarity + 1) / 2.0;
    }
}