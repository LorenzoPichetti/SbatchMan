import re
from pathlib import Path
from copy import deepcopy

from sbatchman.config.global_config import get_cluster_name
from sbatchman.exceptions import ClusterNameNotFoundError, SyntaxError

def load_variable_values(var_value, key):
  # If var_value is a list, return as is
  if isinstance(var_value, list):
    return var_value
  # This may be a `per_cluster`, `map`, or mixed variable
  elif isinstance(var_value, dict):
    if 'per_cluster' in var_value and isinstance(var_value['per_cluster'], dict):
      per_cluster = var_value['per_cluster']

      if get_cluster_name() in per_cluster:
          val = per_cluster[get_cluster_name()]
      elif 'default' in per_cluster:
          val = per_cluster['default']
      elif 'default' in var_value:
          val = var_value['default']
      else:
        raise ClusterNameNotFoundError(f'Cluster "{get_cluster_name()}" not found in {var_value} and no "default" value specified.')

      return load_variable_values(val, key)
    
    elif 'map' in var_value and isinstance(var_value['map'], dict):
      # Store the map structure for later use in substitution
      # Return a special marker that _substitute will recognize
      return {
        '__map__': deepcopy(var_value['map']),
        '__default__': deepcopy(var_value.get('default')),
      }
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
        f"Variable value '{var_value}' (key: {key}) is not a list, file, or directory.\n"
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
    Resolve a map variable recursively.

    Supports:
      map -> value
      map -> per_cluster -> value
      map -> per_cluster -> map -> value
    """

    if not isinstance(map_var_dict, dict) or '__map__' not in map_var_dict:
        return []

    map_dict = {
        str(k): v
        for k, v in map_var_dict['__map__'].items()
    }

    key_str = str(key_value)

    if key_str in map_dict:
        selected = map_dict[key_str]
    elif "default" in map_dict:
        selected = map_dict["default"]
    elif map_var_dict.get("__default__") is not None:
        selected = map_var_dict["__default__"]
    else:
        raise KeyError(
            f"Map variable lookup failed: key '{key_str}' not found "
            "and no default value specified."
        )

    # Resolve nested per_cluster/map structures
    resolved = load_variable_values(
        selected,
        f"<map:{key_str}>"
    )

    # If the selected value is another map, keep resolving
    if isinstance(resolved, dict) and "__map__" in resolved:
        return resolve_map_variable(
            resolved,
            key_value
        )

    return resolved


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
      map_value = variables.get(var_name)
      key_value = variables.get(key_var_name)

      # Unwrap tuples (e.g. from file/dir-based variables) to use as a lookup key
      if isinstance(key_value, tuple):
        key_value = key_value[0]

      if isinstance(map_value, dict) and '__map__' in map_value:
        if key_value is None:
          # key variable not resolved yet, leave for a later pass
          return full_match
        try:
          resolved = resolve_map_variable(map_value, key_value)
        except KeyError:
          return full_match

        if isinstance(resolved, tuple):
          return str(resolved[0])
        if isinstance(resolved, list):
          if len(resolved) == 1:
            return str(resolved[0])
          # A map should resolve to a single scalar per job; if this happens,
          # the YAML's map entry itself needs to be a scalar/1-item list.
          return full_match
        if resolved is not None:
          return str(resolved)
        return full_match

      elif map_value is not None:
        # var_name wasn't actually a map — just substitute it directly
        return str(map_value)

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
