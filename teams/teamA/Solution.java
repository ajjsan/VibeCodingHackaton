import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Production Log Aggregator — Round 1, 2, 4.
 * Раунд 4: PRESSURE_DROP = макс. падение вперёд во времени (max(p_i - p_j), i < j) в окне.
 * Точка входа: stdin → stdout.
 */
public class Solution {

    public static void main(String[] args) {
        try {
            run(
                new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8)),
                new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8))
            );
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    static void run(BufferedReader in, BufferedWriter out) throws IOException {
        List<Query> queries = InputParser.parseQueries(in);
        Map<String, WellStats> wells = InputParser.processData(in);
        QueryExecutor.execute(queries, wells, out);
        out.flush();
    }

    // -------------------------------------------------------------------------
    // Constants
    // -------------------------------------------------------------------------
    static final class Constants {
        private Constants() {}
        static final String SECTION_QUERIES = "QUERIES";
        static final String SECTION_DATA = "DATA";
        static final String SECTION_END = "END";
        static final String PREFIX_AVG_OIL = "AVG_OIL well_id=";
        static final String PREFIX_WATER_CUT = "WATER_CUT well_id=";
        static final String PREFIX_TOP_WELLS = "TOP_WELLS_BY_OIL k=";
        static final String PREFIX_PRESSURE_DROP = "PRESSURE_DROP well_id=";
        static final String PRESSURE_DROP_WINDOW = " window=";
        static final String NA = "NA";
        static final String ZERO_WATER_CUT = "0.000000";
        static final String DECIMAL_FORMAT = "%.6f";
        static final int DATA_FIELDS_COUNT = 5;
    }

    // -------------------------------------------------------------------------
    // Query
    // -------------------------------------------------------------------------
    static final class Query {
        enum Type { AVG_OIL, WATER_CUT, TOP_WELLS_BY_OIL, PRESSURE_DROP }
        final Type type;
        final String wellId;
        final int k;
        final int windowSize;

        Query(Type type, String wellId, int k, int windowSize) {
            this.type = type;
            this.wellId = wellId;
            this.k = k;
            this.windowSize = windowSize;
        }
        static Query avgOil(String wellId) { return new Query(Type.AVG_OIL, wellId, -1, -1); }
        static Query waterCut(String wellId) { return new Query(Type.WATER_CUT, wellId, -1, -1); }
        static Query topWellsByOil(int k) { return new Query(Type.TOP_WELLS_BY_OIL, null, k, -1); }
        static Query pressureDrop(String wellId, int windowSize) { return new Query(Type.PRESSURE_DROP, wellId, -1, windowSize); }
        Type getType() { return type; }
        String getWellId() { return wellId; }
        int getK() { return k; }
        int getWindowSize() { return windowSize; }
    }

    // -------------------------------------------------------------------------
    // WellStats
    // -------------------------------------------------------------------------
    static final class WellStats {
        private double sumOil;
        private double sumWater;
        private int count;
        private long lastTs = -1;
        private double lastOil;
        private double lastWater;
        private final List<Double> pressures = new ArrayList<>();

        void add(long timestamp, double oilRate, double waterRate, double pressure) {
            if (lastTs >= 0 && lastTs == timestamp) {
                sumOil -= lastOil;
                sumWater -= lastWater;
                pressures.set(pressures.size() - 1, pressure);
            } else {
                count++;
                pressures.add(pressure);
            }
            sumOil += oilRate;
            sumWater += waterRate;
            lastTs = timestamp;
            lastOil = oilRate;
            lastWater = waterRate;
        }
        double getSumOil() { return sumOil; }
        double getSumWater() { return sumWater; }
        int getCount() { return count; }
        double getAvgOil() { return count == 0 ? 0 : sumOil / count; }
        double getWaterCut() { double liquid = sumOil + sumWater; return liquid == 0 ? 0 : sumWater / liquid; }
        List<Double> getPressures() { return pressures; }
    }

    // -------------------------------------------------------------------------
    // InputParser
    // -------------------------------------------------------------------------
    static final class InputParser {
        static List<Query> parseQueries(BufferedReader in) throws IOException {
            List<Query> list = new ArrayList<>();
            String line = in.readLine();
            if (line == null || !line.trim().equals(Constants.SECTION_QUERIES)) return list;
            while (true) {
                line = in.readLine();
                if (line == null) break;
                line = line.trim();
                if (line.equals(Constants.SECTION_DATA)) break;
                if (line.isEmpty()) continue;
                Query q = parseQueryLine(line);
                if (q != null) list.add(q);
            }
            return list;
        }

        static Map<String, WellStats> processData(BufferedReader in) throws IOException {
            Map<String, WellStats> wells = new HashMap<>();
            String line;
            while ((line = in.readLine()) != null) {
                line = line.trim();
                if (line.equals(Constants.SECTION_END)) break;
                if (line.isEmpty()) continue;
                processDataLine(line, wells);
            }
            return wells;
        }

        private static Query parseQueryLine(String line) {
            if (line.startsWith(Constants.PREFIX_AVG_OIL))
                return Query.avgOil(line.substring(Constants.PREFIX_AVG_OIL.length()).trim());
            if (line.startsWith(Constants.PREFIX_WATER_CUT))
                return Query.waterCut(line.substring(Constants.PREFIX_WATER_CUT.length()).trim());
            if (line.startsWith(Constants.PREFIX_TOP_WELLS)) {
                String rest = line.substring(Constants.PREFIX_TOP_WELLS.length()).trim();
                try { return Query.topWellsByOil(Integer.parseInt(rest)); } catch (NumberFormatException e) { return null; }
            }
            if (line.startsWith(Constants.PREFIX_PRESSURE_DROP)) {
                String rest = line.substring(Constants.PREFIX_PRESSURE_DROP.length()).trim();
                int idx = rest.indexOf(Constants.PRESSURE_DROP_WINDOW);
                if (idx < 0) return null;
                String wellId = rest.substring(0, idx).trim();
                try {
                    int w = Integer.parseInt(rest.substring(idx + Constants.PRESSURE_DROP_WINDOW.length()).trim());
                    return Query.pressureDrop(wellId, w);
                } catch (NumberFormatException e) { return null; }
            }
            return null;
        }

        private static void processDataLine(String line, Map<String, WellStats> wells) {
            String[] parts = line.split(",", -1);
            if (parts.length != Constants.DATA_FIELDS_COUNT) return;
            String wellId = parts[0].trim();
            if (wellId.isEmpty()) return;
            long ts;
            double oil, water, pressure;
            try {
                ts = Long.parseLong(parts[1].trim());
                oil = Double.parseDouble(parts[2].trim());
                water = Double.parseDouble(parts[3].trim());
                pressure = Double.parseDouble(parts[4].trim());
            } catch (NumberFormatException e) { return; }
            if (oil < 0 || water < 0) return;
            wells.computeIfAbsent(wellId, k -> new WellStats()).add(ts, oil, water, pressure);
        }
    }

    // -------------------------------------------------------------------------
    // QueryExecutor
    // -------------------------------------------------------------------------
    static final class QueryExecutor {
        static void execute(List<Query> queries, Map<String, WellStats> wells, BufferedWriter out) throws IOException {
            for (Query q : queries) {
                out.write(answerOne(q, wells));
                out.newLine();
            }
        }

        private static String answerOne(Query q, Map<String, WellStats> wells) {
            switch (q.getType()) {
                case AVG_OIL: return answerAvgOil(q.getWellId(), wells);
                case WATER_CUT: return answerWaterCut(q.getWellId(), wells);
                case TOP_WELLS_BY_OIL: return answerTopWellsByOil(q.getK(), wells);
                case PRESSURE_DROP: return answerPressureDrop(q.getWellId(), q.getWindowSize(), wells);
                default: return Constants.NA;
            }
        }

        private static String answerAvgOil(String wellId, Map<String, WellStats> wells) {
            WellStats w = wells.get(wellId);
            if (w == null || w.getCount() == 0) return Constants.NA;
            return formatDouble(w.getAvgOil());
        }

        private static String answerWaterCut(String wellId, Map<String, WellStats> wells) {
            WellStats w = wells.get(wellId);
            if (w == null) return Constants.NA;
            if (w.getSumOil() + w.getSumWater() == 0) return Constants.ZERO_WATER_CUT;
            return formatDouble(w.getWaterCut());
        }

        private static String answerTopWellsByOil(int k, Map<String, WellStats> wells) {
            if (wells.isEmpty()) return "";
            return wells.entrySet().stream()
                .sorted(Comparator.<Map.Entry<String, WellStats>>comparingDouble(e -> -e.getValue().getSumOil()).thenComparing(Map.Entry::getKey))
                .limit(k)
                .map(Map.Entry::getKey)
                .collect(Collectors.joining(","));
        }

        /**
         * Раунд 4: максимальное падение давления вперёд во времени внутри окна.
         * max(pressure_i - pressure_j) по всем окнам, где i < j (j позже по времени).
         * Только падения; если давление только растёт — 0.
         */
        private static String answerPressureDrop(String wellId, int windowSize, Map<String, WellStats> wells) {
            if (windowSize <= 0) return Constants.NA;
            WellStats w = wells.get(wellId);
            if (w == null) return Constants.NA;
            List<Double> p = w.getPressures();
            if (p.size() < windowSize) return Constants.NA;
            return formatDouble(slidingWindowMaxForwardDrop(p, windowSize));
        }

        /** Максимум по всем окнам размера w значения max(p_i - p_j) при i < j внутри окна. */
        private static double slidingWindowMaxForwardDrop(List<Double> a, int w) {
            double best = 0;
            for (int s = 0; s + w <= a.size(); s++) {
                double runMax = a.get(s);
                for (int j = 1; j < w; j++) {
                    double drop = runMax - a.get(s + j);
                    if (drop > best) best = drop;
                    if (a.get(s + j) > runMax) runMax = a.get(s + j);
                }
            }
            return best;
        }

        private static String formatDouble(double value) {
            return String.format(Locale.US, Constants.DECIMAL_FORMAT, value);
        }
    }
}
