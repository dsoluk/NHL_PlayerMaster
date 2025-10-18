

import csv
import requests
import json
import unicodedata
import jellyfish  # pip install jellyfish for fuzzy string matching

class PlayerRegistry:
    def __init__(self):
        self.players = {}  # master_id -> {source_ids: {}, canonical_name: "", attributes: {}}
        self.next_id = 1
    
    def register_player(self, source, source_id, name, attributes=None):
        """
        Register a player from a data source, generating or retrieving a master ID.
        
        Args:
            source: The data source name (e.g., 'nhl_api', 'hockey_reference')
            source_id: ID from the source (or None if unavailable)
            name: Player name from the source
            attributes: Dict of additional attributes (birth_date, position, etc.)
            
        Returns:
            (master_id, created): Tuple where created=True if a new master record was created
        """
        if attributes is None:
            attributes = {}
        
        # Do not use volatile team fields to influence matching
        volatile_keys = {'last_seen_teamAbbrev', 'last_seen_teamName'}
        
        # Check if player already exists by source_id
        for master_id, player_data in self.players.items():
            if source in player_data['source_ids'] and player_data['source_ids'][source] == source_id:
                # Update non-identifying rolling fields
                for k, v in attributes.items():
                    if k in volatile_keys:
                        player_data['attributes'][k] = v
                return master_id, False
        
        # If no match by ID, try fuzzy name matching with additional attributes
        normalized_name = self._normalize_name(name)
        for master_id, player_data in self.players.items():
            # Skip if we're sure this is a different player by having different IDs for same source
            if source in player_data['source_ids'] and player_data['source_ids'][source] != source_id and source_id is not None:
                continue
                
            # Check name similarity
            existing_name = player_data['canonical_name']
            similarity = jellyfish.jaro_winkler_similarity(normalized_name, existing_name)
            
            # Boost similarity if attributes match (ignore volatile team fields)
            for attr, value in attributes.items():
                if attr in volatile_keys:
                    continue
                if attr in player_data['attributes'] and player_data['attributes'][attr] == value:
                    similarity += 0.1  # Boost for each matching attribute
            
            if similarity > 0.9:  # Threshold for considering a match
                # Update with the new source ID
                player_data['source_ids'][source] = source_id
                # Update rolling team fields if provided
                for k, v in attributes.items():
                    if k in volatile_keys:
                        player_data['attributes'][k] = v
                return master_id, False
        
        # No match found, create new player
        master_id = self._generate_id()
        # Persist attributes including volatile fields as the initial snapshot
        self.players[master_id] = {
            'source_ids': {source: source_id},
            'canonical_name': normalized_name,
            'attributes': attributes
        }
        return master_id, True
    
    def _normalize_name(self, name):
        """Normalize player name by removing accents and standardizing format"""
        # Convert to lowercase
        name = name.lower()
        
        # Remove accents
        name = unicodedata.normalize('NFD', name).encode('ascii', 'ignore').decode('utf-8')
        
        # Handle common nicknames (could be expanded)
        nickname_map = {
            'alex': 'alexander',
            'mike': 'michael',
            # Add more as needed
        }
        
        # Split into parts and replace nicknames
        parts = name.split()
        if len(parts) > 0 and parts[0] in nickname_map:
            parts[0] = nickname_map[parts[0]]
        
        return ' '.join(parts)
    
    def _generate_id(self):
        """Generate a unique master ID for a new player"""
        master_id = self.next_id
        self.next_id += 1
        return master_id
    
    def get_player_sources(self, master_id):
        """Get all source IDs for a given master ID"""
        if master_id in self.players:
            return self.players[master_id]['source_ids']
        return None
    
    def get_canonical_name(self, master_id):
        """Get the canonical name for a player"""
        if master_id in self.players:
            return self.players[master_id]['canonical_name']
        return None
    
    def export_to_json(self, filename='player_registry.json'):
        """Export the entire player registry to JSON"""
        with open(filename, 'w') as f:
            json.dump(self.players, f, indent=2)
    
    def import_from_json(self, filename='player_registry.json'):
        """Import player registry from JSON"""
        try:
            with open(filename, 'r') as f:
                data = json.load(f)
                self.players = data
                # Ensure next_id is higher than any existing ID
                self.next_id = max([int(k) for k in self.players.keys()], default=0) + 1
        except FileNotFoundError:
            print(f"Registry file {filename} not found, starting with empty registry")

def get_data_from_url(team_abbrev):
    # Defensive sanitize in case input contains BOM/whitespace or mixed case
    if isinstance(team_abbrev, str):
        team_abbrev = team_abbrev.replace('\ufeff', '').strip().upper()
    url = f"https://api-web.nhle.com/v1/roster/{team_abbrev}/current"
    response = requests.get(url)
    print(url)

    if response.status_code != 200:
        print(f"Failed to get data for {team_abbrev}, status code: {response.status_code}")
        return None

    return response.json()


def read_team_abbrev_from_csv():
    team_abbreviations = []
    # Use utf-8-sig to automatically strip a UTF-8 BOM if present
    with open('Team2TM.csv', 'r', encoding='utf-8-sig', newline='') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            if not row:
                continue  # skip empty lines
            abbrev = str(row[0]).replace('\ufeff', '').strip().upper()
            if abbrev:
                team_abbreviations.append(abbrev)

    return team_abbreviations


def process_player_data(data, registry, source="nhl_api"):
    """Extract and register players from NHL API response data (robust to schema variants).
    Returns (processed_data, summary) where summary includes per-team and total new vs existing counts.
    """
    def _name_val(v):
        # Handle names that may be strings or objects like {"default": "John"}
        if isinstance(v, dict):
            return v.get("default") or v.get("first", "")
        return v or ""

    def _team_name(team):
        tn = team.get("teamName") or team.get("teamCommonName") or {}
        return _name_val(tn) if isinstance(tn, (dict, str)) else ""

    def _collect_players(team):
        # Different endpoints/layouts: some have consolidated 'roster', many have 'forwards','defensemen','goalies'
        buckets = []
        for key in ["roster", "forwards", "defensemen", "defense", "goalies", "skaters"]:
            val = team.get(key)
            if isinstance(val, list):
                buckets.append(val)
        players = []
        for bucket in buckets:
            for p in bucket:
                players.append(p)
        return players

    processed_data = []
    summary = {"totals": {"new": 0, "existing": 0}, "teams": []}

    for team_data in data:
        if not isinstance(team_data, dict):
            # Skip unexpected entries
            continue

        team_info = {
            "teamName": _team_name(team_data),
            "teamAbbrev": team_data.get("teamAbbrev", ""),
            "players": []
        }

        team_new = 0
        team_existing = 0

        players = _collect_players(team_data)
        processed_count = 0

        for player in players:
            # Extract robust ID field
            source_id = player.get("id") or player.get("playerId") or player.get("playerID")

            # Names may be nested objects
            first = _name_val(player.get("firstName"))
            last = _name_val(player.get("lastName"))
            name = (f"{first} {last}").strip()

            # Jersey/sweater number can be named differently
            jersey = player.get("jerseyNumber") or player.get("sweaterNumber") or ""

            position = player.get("positionCode") or player.get("position") or ""

            player_info = {
                "source_id": source_id,
                "name": name,
                "attributes": {
                    "position": position,
                    "jersey_number": jersey,
                    # Rolling fields to update in registry without affecting identity
                    "last_seen_teamAbbrev": team_info["teamAbbrev"],
                    "last_seen_teamName": team_info["teamName"],
                }
            }

            # Register player and get master ID (only if we have at least a name or an ID)
            if source_id is not None or name:
                master_id, created = registry.register_player(
                    source,
                    player_info["source_id"],
                    player_info["name"],
                    player_info["attributes"]
                )
                player_info["master_id"] = master_id
                processed_count += 1
                if created:
                    team_new += 1
                else:
                    team_existing += 1

            team_info["players"].append(player_info)

        # Optional lightweight count for transparency
        if team_info["teamAbbrev"] or team_info["teamName"]:
            print(f"Processed {processed_count} players for {team_info['teamAbbrev'] or team_info['teamName']} (new: {team_new}, existing: {team_existing})")

        summary["teams"].append({
            "teamAbbrev": team_info["teamAbbrev"],
            "teamName": team_info["teamName"],
            "new": team_new,
            "existing": team_existing,
            "total": processed_count
        })
        summary["totals"]["new"] += team_new
        summary["totals"]["existing"] += team_existing

        processed_data.append(team_info)

    return processed_data, summary


def main():
    # Initialize player registry
    registry = PlayerRegistry()
    
    # Try to load existing registry
    registry.import_from_json()

    
    # Get data from NHL API
    team_abbreviations = read_team_abbrev_from_csv()
    data_list = []  # initialize an empty list to store all team data
    for team_abbrev in team_abbreviations:
        data = get_data_from_url(team_abbrev)
        if data is not None:
            data_list.append(data)  # append each team's data to the list

    # Optionally save the raw combined team data for transparency/debugging
    with open('all_teams_data.json', 'w', encoding='utf-8') as file:
        json.dump(data_list, file, indent=2)

    # Process player data and register players
    processed_data, summary = process_player_data(data_list, registry)

    # Save the processed data with assigned master IDs
    with open('all_teams_data_with_master_ids.json', 'w', encoding='utf-8') as file:
        json.dump(processed_data, file, indent=2)

    # Save a run summary for visibility (new vs existing)
    with open('run_summary.json', 'w', encoding='utf-8') as file:
        json.dump(summary, file, indent=2)
    
    # Save the updated player registry
    registry.export_to_json()


if __name__ == "__main__":
    main()
