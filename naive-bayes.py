from mrjob.job import MRJob
from mrjob.step import MRStep

class NaiveBayes(MRJob):
    model = {}

    def steps(self):
        return [
            MRStep(mapper=self.mapper_phase1, reducer=self.reducer_phase1),
            MRStep(mapper=self.mapper_phase2, reducer=self.reducer_phase2)
        ]   

    def mapper_phase1(self, _, line):
        # <Data1><,><Data2><,><...><DataM><,><Class>
        tokens = line.split(',')

        classIndex = len(tokens) - 1
        theClass = tokens[classIndex]

        for i in range(classIndex):
            reduceKey = (tokens[i], theClass)
            yield reduceKey, 1

        reduceKey = ("CLASS", theClass)

        yield reduceKey, 1

    def reducer_phase1(sellf, key, values):
        yield key, sum(values)

    def mapper_phase2(self, key, value):
        condition, probes = key

        yield condition, (probes, value)

    def reducer_phase2(self, key, values):
        items = list(values)

        theClass = key
        if len(items) == 2:
            first = items[0][0]
            v1 = items[0][1]

            second = items[1][0]
            v2 = items[1][1]

            yield (key, first), (v1 / (v1 + v2))
            self.model[(key, first)] = (v1 / (v1 + v2))

            yield (key, second), (v2 / (v1 + v2))
            self.model[(key, second)] = (v2 / (v1 + v2))

if __name__ == '__main__':
    NaiveBayes.run()

    model = NaiveBayes.model

    instance = ["Sunny", "Cool", "High", "Strong"]

    yes = 1.0
    no = 1.0
    for feature in instance:
        yes = yes * model[(feature, "Yes")]
        no = no * model[(feature, "No")]

    yes = yes * model[("CLASS", "Yes")]
    no = no * model[("CLASS", "No")]

    if yes > no:
        print(str(instance) + " : " + str("Yes"))
    else:
        print(str(instance) + " : " + str("No"))

    