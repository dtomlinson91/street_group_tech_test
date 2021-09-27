# Approach

The general approach to the pipeline is:

## Loading stage

- Load using `#!python beam.io.ReadFromText()`
- Split the string loaded by `,` as it's a comma delimited `.csv`.
- Strip the leading/trailing `"` marks.

The result is an array with each element representing a single column in that row.

## Cleaning stage

Already discussed.

## Create a mapping table

The mapping table takes each row and creates a `(key,value)` pair with:

- The key being the id across all columns (`id_all_columns`).
- The value being the raw data as an array.

The mapping table is then condensed to a single dictionary with these key, value pairs and is used as a side input further down the pipeline.

This mapping table is created to ensure the `GroupByKey` operation is as quick as possible. The more data you have to process in a `GroupByKey`, the longer the operation takes. By doing the `GroupByKey` using just the ids, the pipeline can process the files much quicker than if we included the raw data in this operation.

## Prepare stage

- Take the mapping table data (before it is condensed) and create a unique id ignoring the price and date (`id_without_price_date`).

This id will not be unique: for properties with more than one transaction they will share this id.

- Create a `(key, value)` pair with:
    - The key being `id_without_price_date`.
    - The value being `id_all_columns`.
- Group by `id_without_price_date`.

This results in a PCollection that looks like: `(id_without_price_date, [id_all_columns,...])`

- Deduplicate the `id_all_columns` inside this array to eliminate repeated rows that are exactly the same.
- Use the mapping table as a side input to reinsert the raw data using the `id_all_columns`.

<details>
    <summary>Example for No.1 B90 3LA</summary>

Mapping table (pre condensed):

```json
('fd4634faec47c29de40bbf7840723b41', ['317500', '2020-11-13 00:00', 'B90 3LA', '1', '', 'VERSTONE ROAD', 'SHIRLEY', 'SOLIHULL', 'SOLIHULL', 'WEST MIDLANDS', ''])
('fd4634faec47c29de40bbf7840723b41', ['317500', '2020-11-13 00:00', 'B90 3LA', '1', '', 'VERSTONE ROAD', 'SHIRLEY', 'SOLIHULL', 'SOLIHULL', 'WEST MIDLANDS', ''])
```

Mapping table (condensed):

```json
{'fd4634faec47c29de40bbf7840723b41': ['317500', '2020-11-13 00:00', 'B90 3LA', '1', '', 'VERSTONE ROAD', 'SHIRLEY', 'SOLIHULL', 'SOLIHULL', 'WEST MIDLANDS', '']}
```

Prepared (key, value):

```json
('fe205bfe66bc7f18c50c8f3d77ec3e30', 'fd4634faec47c29de40bbf7840723b41')
('fe205bfe66bc7f18c50c8f3d77ec3e30', 'fd4634faec47c29de40bbf7840723b41')
```

Prepared (GroupByKey):

```json
('fe205bfe66bc7f18c50c8f3d77ec3e30', ['fd4634faec47c29de40bbf7840723b41', 'fd4634faec47c29de40bbf7840723b41'])
```

Prepared (Deduplicated):

```json
('fe205bfe66bc7f18c50c8f3d77ec3e30', ['fd4634faec47c29de40bbf7840723b41'])
```

Use mapping table as side input:

```json
('fe205bfe66bc7f18c50c8f3d77ec3e30', ['317500', '2020-11-13 00:00', 'B90 3LA', '1', '', 'VERSTONE ROAD', 'SHIRLEY', 'SOLIHULL', 'SOLIHULL', 'WEST MIDLANDS', ''])
```

</details>

## Format stage

This stage takes the result and constructs a `json` object out of the grouped data. The schema for this output is discussed in the following page.

## Save stage

- The PCollection is combined with `#!python beam.combiners.ToList()`
- Apply `json.dumps()` for proper quotation marks for strings.
- Write to text with `#!python beam.io.WriteToText`.
