# Tokyo-CDR-data-processing
## Dataset description
The Docomo CDR dataset stores estimated amount of population flow, as the volume of people movement from origin mesh to destination mesh. The origin and destination meshes are decided when a person stayed more than an hour in a specific mesh.

The time span is a month. Granularity is 10 minutes and 500 memters for temporal and spatial, respectively.

## My work
I processed the 100G CSV data file, sorted by time with SparkSQL, answered exploratory data questions, and prepared travel demand matrices in .txt and .npz format for later analysis.

## Files in this repository
1. **sortByTime.py** sorts the dataset with SparkSQL and produces sorted data in several CSV files.
2. **tokyo.ipynb** is the notebook where I answered most research questions and where I provide the gist of the processing task.
3. **writeByTime.py** finally defines the npz and txt writing functions and these functions are applied in the Jupyter Notebook.

## References
Please refer to these papers for idea of the project.
```
@inproceedings{akagi2018fast,
  title={A Fast and Accurate Method for Estimating People Flow from Spatiotemporal Population Data.},
  author={Akagi, Yasunori and Nishimura, Takuya and Kurashima, Takeshi and Toda, Hiroyuki},
  booktitle={IJCAI},
  pages={3293--3300},
  year={2018}
}
```
