import * as yaml from 'js-yaml';

export async function fetchYamlToJSON(filePath: string): Promise<any> {
  return fetch(filePath)
    .then((res) => res.text())
    .then((res) => yaml.load(res));
}
