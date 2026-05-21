import re
from pathlib import Path

from sbatchman.config.global_config import get_cluster_name
from sbatchman.exceptions import ClusterNameNotFoundError, SyntaxError

def load_variable_values(var_value):
  # If var_value is a list, return as is
  if isinstance(var_value, list):
    return var_value
  # This may be a `per_cluster`, `map`, or mixed variable
  elif isinstance(var_value, dict):
    if 'per_cluster' in var_value and isinstance(var_value['per_cluster'], dict):
      val = var_value['per_cluster'].get(get_cluster_name()) or var_value.get('default')
      if not val: 
        raise ClusterNameNotFoundError(f'Cluster "{get_cluster_name()}" not found in {var_value} and no "default" value specified.')
      return load_variable_values(val)
    elif 'map' in var_value and isinstance(var_value['map'], dict):
      # Store the map structure for later use in substitution
      # Return a special marker that _substitute will recognize
      return {'__map__': var_value['map'], '__default__': var_value.get('default')}
    else:
      raise SyntaxError(f"Invalid variable value {var_value}. In case the variable value is a dictionary, you must specify a 'per_cluster' dictionary, a 'map' dictionary, or both (+ optional default value).")
  # If var_value is a string and a file, read lines
  elif isinstance(var_value, str):
    path = Path(var_value)
    if path.is_file():
      with open(path, "r") as f:
        return [line.strip().replace('\n', '') for line in f if line.strip()]
    elif path.is_dir():
      # Return sorted list of file names in the directory
      return sorted([(str(p.absolute()), p.stem) for p in path.iterdir() if p.is_file()])
    else:
      raise SyntaxError(
        f"Variable value '{var_value}' is not a list, file, or directory.\n"
        "YAML script semantics:\n"
        "- Variables can be lists, map/per_cluster objects, a path to a file (one value per line), or a path to a directory (all file absolute paths used as values).\n"
        "- The cartesian product of all variable values is used to generate jobs.\n"
        "- Experiments can define configuration names (possibly using variables) and tags.\n"
        "- 'command' and 'variables' can be redefined or extended in inner YAML tags.\n"
        "- The '{var_name}' syntax is substituted with the actual value of 'var_name'."
      )
  else:
    return [var_value]


def map_info_to_vars(map_info):
  """Convert map_info dict to variables dict with map structures."""
  return {k: v[0] for k, v in map_info.items()}


def resolve_map_variable(map_var_dict, key_value):
  """
  Resolve a map variable to its value list given a key.
  
  Args:
    map_var_dict: A dict with '__map__' and '__default__' keys
    key_value: The key to look up in the map
    
  Returns:
    A list of values from the map, or the default list
  """
  if not isinstance(map_var_dict, dict) or '__map__' not in map_var_dict:
    return []
  
  map_dict = {str(k): v for k, v in map_var_dict['__map__'].items()}
  key_str = str(key_value)
  
  if key_str in map_dict:
    result = map_dict[key_str]
    # Ensure result is a list
    if isinstance(result, list):
      return result
    else:
      return [result]
  
  # Fall back to default
  if '__default__' in map_var_dict and map_var_dict['__default__'] is not None:
    default = map_var_dict['__default__']
    if isinstance(default, list):
      return default
    else:
      return [default]
  
  # Key not found and no default
  raise KeyError(
    f"Map variable lookup failed: key '{key_str}' not found in map and no default value specified."
  )


def substitute(template, variables):
  """
  Replace occurrences of:
    - {var_name}
    - {map_var[key_var]}

  Rules:
    - var_name contains no spaces or newlines
    - map_var[key_var] uses the value of key_var as the lookup key in map_var
    - pattern is not preceded by $
    - if variable value is a tuple:
        * {var_name} -> first element
        * {var_name_filename} -> second element
  """
  if not isinstance(template, str):
    return template

  substitution_pattern = re.compile(
    r'(?<!\$)\{([^\s{}\[\]]+)(?:\[([^\s\[\]]+)\])?\}'
  )

  def replacer(match):
    full_match = match.group(0)
    var_name = match.group(1)
    key_var_name = match.group(2)

    # Handle {map_var[key_var]}
    if key_var_name:
      value = variables.get(var_name)

      if value is not None:
        return str(value)

      return full_match

    # Handle {var_name_filename}
    if var_name.endswith("_filename"):
      base_name = var_name[:-len("_filename")]
      value = variables.get(base_name)

      if isinstance(value, tuple) and len(value) > 1:
        return str(value[1])

    else:
      # Handle regular {var_name}
      value = variables.get(var_name)

      if isinstance(value, tuple):
        return str(value[0])

      elif isinstance(value, dict):
        # Don't substitute raw dicts directly
        return full_match

      elif value is not None:
        return str(value)

    # Leave unresolved variables unchanged
    return full_match

  return substitution_pattern.sub(replacer, template)


def extract_used_vars(*templates):
  """
  Extract variable names used in {var} format or {map_var[key_var]} format from given templates.
  Returns a set of variable names that are referenced.
  """
  var_names = set()
  for template in templates:
    if isinstance(template, str):
      # Match both {var_name} and {map_var[key_var]} patterns
      matches = re.findall(r"{(\w+)(?:\[(\w+)\])?}", template)
      for match in matches:
        # match is a tuple like ('var_name', 'key_var') or ('var_name', '')
        var_names.add(match[0])  # Add the variable/map name
        if match[1]:  # If there's a key variable
          var_names.add(match[1])  # Add the key variable
  return var_names
