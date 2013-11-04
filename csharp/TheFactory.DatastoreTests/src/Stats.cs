using System;
using System.IO;

namespace TheFactory.DatastoreTests {
    // port of leveldb's Stats class
    public class Stats {
        long start;
        long finish;
        double seconds;
        int done;
        int next_report;
        long bytes;
        long last_op_finish;
        Histogram hist = new Histogram();
        string message;

        public Stats () {
        }

        public void Start() {
            next_report = 100;
            hist.Clear();
            done = 0;
            bytes = 0;
            seconds = 0;
            start = DateTime.Now.Ticks;
            last_op_finish = start;
            finish = start;
            message = "";
        }

        public void Merge(Stats other) {
            hist.Merge(other.hist);
            done += other.done;
            bytes += other.bytes;
            seconds += other.seconds;
            if (other.start < start) {
                start = other.start;
            }
            if (other.finish > finish) {
                finish = other.finish;
            }

            if (message.Length == 0) {
                message = other.message;
            }
        }

        public void Finish() {
            finish = DateTime.Now.Ticks;
            seconds = (finish - start) * 1e-7;
        }

        public void AddMessage(String msg) {
            message = message + " " + msg;
        }

        public void FinishedSingleOp() {
            long now = DateTime.Now.Ticks;

            double micros = (double)(now - last_op_finish) * 1e-1;
            hist.Add(micros);
            if (micros > 20000) {
                Console.WriteLine("long op: {0:F1} micros", micros);
            }

            last_op_finish = now;

            done++;
            if (done >= next_report) {
                if (next_report < 1000)
                    next_report += 100;
                else if (next_report < 5000)
                    next_report += 500;
                else if (next_report < 10000)
                    next_report += 1000;
                else if (next_report < 50000)
                    next_report += 5000;
                else if (next_report < 100000)
                    next_report += 10000;
                else if (next_report < 500000)
                    next_report += 5000;
                else
                    next_report += 10000;

                Console.WriteLine("... finished {0:d} ops", done);
            }
        }

        public void AddBytes(long n) {
            bytes += n;
        }

        public String Report(String name) {
            if (done < 1)
                done = 1;

            var report = new StringWriter();

            String rate = "";
            if (bytes > 0) {
                double elapsed = (float)(finish - start) * 1e-7;
                rate = String.Format("{0,6:F1} MB/s", (bytes / 1048576.0) / elapsed);
            }

            report.Write(String.Format("{0,-12} : {1,11:F3} micros/op; {2}\n", name,
                                       seconds * 1e6 / done, rate + message));

            report.Write(hist.Report());

            return report.ToString();
        }
    }

    public class Histogram {
        double min;
        double max;
        double num;
        double sum;
        double sum_squares;
        double[] buckets;

        readonly double[] limits = {
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18, 20, 25, 30, 35, 40, 45,
            50, 60, 70, 80, 90, 100, 120, 140, 160, 180, 200, 250, 300, 350, 400, 450,
            500, 600, 700, 800, 900, 1000, 1200, 1400, 1600, 1800, 2000, 2500, 3000,
            3500, 4000, 4500, 5000, 6000, 7000, 8000, 9000, 10000, 12000, 14000,
            16000, 18000, 20000, 25000, 30000, 35000, 40000, 45000, 50000, 60000,
            70000, 80000, 90000, 100000, 120000, 140000, 160000, 180000, 200000,
            250000, 300000, 350000, 400000, 450000, 500000, 600000, 700000, 800000,
            900000, 1000000, 1200000, 1400000, 1600000, 1800000, 2000000, 2500000,
            3000000, 3500000, 4000000, 4500000, 5000000, 6000000, 7000000, 8000000,
            9000000, 10000000, 12000000, 14000000, 16000000, 18000000, 20000000,
            25000000, 30000000, 35000000, 40000000, 45000000, 50000000, 60000000,
            70000000, 80000000, 90000000, 100000000, 120000000, 140000000, 160000000,
            180000000, 200000000, 250000000, 300000000, 350000000, 400000000,
            450000000, 500000000, 600000000, 700000000, 800000000, 900000000,
            1000000000, 1200000000, 1400000000, 1600000000, 1800000000, 2000000000,
            2500000000.0, 3000000000.0, 3500000000.0, 4000000000.0, 4500000000.0,
            5000000000.0, 6000000000.0, 7000000000.0, 8000000000.0, 9000000000.0,
            1e200,
        };

        public Histogram() {
            buckets = new double[limits.Length];
        }

        public void Clear() {
            min = limits[limits.Length - 1];
            max = 0;
            num = 0;
            sum = 0;
            sum_squares = 0;
            for (int i=0; i<buckets.Length; i++) {
                buckets[i] = 0;
            }
        }

        public void Add(double value) {
            int b = 0;
            while (b < limits.Length - 1 && limits[b] <= value) {
                b++;
            }
            buckets[b] += 1.0;
            if (min > value)
                min = value;
            if (max < value)
                max = value;

            num += 1;
            sum += value;
            sum_squares += (value * value);
        }

        public void Merge(Histogram other) {
            if (other.min < min)
                min = other.min;

            if (other.max > max)
                max = other.max;

            num += other.num;
            sum += other.sum;
            sum_squares += other.sum_squares;
            for (int b=0; b<buckets.Length; b++) {
                buckets[b] += other.buckets[b];
            }
        }

        public double Median() {
            return Percentile(50.0);
        }

        public double Percentile(double p) {
            double threshold = num * (p / 100.0);
            double sum = 0;
            for (int b = 0; b < buckets.Length; b++) {
                sum += buckets[b];
                if (sum >= threshold) {
                    double left_point = (b == 0) ? 0 : limits[b - 1];
                    double right_point = limits[b];
                    double left_sum = sum - buckets[b];
                    double right_sum = sum;
                    double pos = (threshold - left_sum) / (right_sum - left_sum);
                    double r = left_point + (right_point - left_point) * pos;
                    if (r < min)
                        r = min;
                    if (r > max)
                        r = max;
                    return r;
                }
            }
            return max;
        }

        public double Average() {
            if (num == 0.0)
                return 0;
            return sum / num;
        }

        public double StandardDeviation() {
            if (num == 0.0)
                return 0;
            double variance = (sum_squares * num - sum * sum) / (num * num);
            return Math.Sqrt(variance);
        }

        public String Report() {
            var buf = new StringWriter();

            buf.Write(String.Format("Count: {0,7:d}  Average: {1,-7:F4}  StdDev: {2,-7:F2}\n",
                                    (int)num, Average(), StandardDeviation()));
            buf.Write(String.Format("Min:   {0,7:F4}  Median:  {1,-7:F4}  Max: {2,-7:F4}\n",
                                    (num == 0.0 ? 0.0 : min), Median(), max));
            buf.Write("-------------------------------------------------------------------\n");

            double mult = 100.0 / num;
            double sum = 0;
            for (int b = 0; b < buckets.Length; b++) {
                if (buckets[b] <= 0.0)
                    continue;
                sum += buckets[b];
                buf.Write(String.Format("[ {0,7:d}, {1,7:d} ) {2,7:d} {3,7:F3}% {4,7:F3}% ",
                                        (int)((b == 0) ? 0.0 : limits[b - 1]),
                                        (int)limits[b],
                                        (int)buckets[b], // count
                                        mult * buckets[b], // percentage
                                        mult * sum)); // cumulative percentage

                // add hash marks based on percentage
                int marks = (int)(20 * (buckets[b] / num) + 0.5);
                for (int i=0; i<marks; i++) {
                    buf.Write("#");
                }
                buf.Write("\n");
            }

            return buf.ToString();
        }
    }
}

