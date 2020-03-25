import apache_beam as beam
import re
import os


#def run(cmd):
#    print('>> {}'.format(cmd))
#    !{cmd}
#    print('')


inputs_pattern = 'data/*'
outputs_prefix = 'output/part'

with beam.Pipeline() as pipeline:
    (
        pipeline
            | 'Read lines' >> beam.io.ReadFromText(inputs_pattern)
            | 'Find words' >> beam.FlatMap(lambda line: re.findall(r"[a-zA-Z]+", line))
            | 'Pair words with 1' >> beam.Map(lambda word: (word, 1))
            | 'Group and sum' >> beam.CombinePerKey(sum)
            | 'Format results' >> beam.Map(lambda word_count: str(word_count))
            | 'Write results' >> beam.io.WriteToText(outputs_prefix)
    )

os.system('head -n 20 {}-00000-of-*'.format(outputs_prefix))