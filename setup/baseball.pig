-- This code is made available under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
-- WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
-- License for the specific language governing permissions and limitations
-- under the License.

register 'udf.jar';
-- Load teams and flatten out the list of players on each team so I can
-- determine which team each player is from.
teams              = load 'baseball_team.tsv' as (name, id, division,
                        league, current_roster, current_manager,
                        historical_roster, historical_managers,
                        current_coaches, historical_coaches, team_stats);
teams_with_players = filter teams by current_roster is not null;
-- Replace the comma separator with white space so I can use TOKENIZE to
-- turn the list into a bag.
temp               = foreach teams_with_players generate name,
                        REPLACE(current_roster, ',', ' ') as current_roster;
team_players       = foreach temp generate name,
                        flatten(TOKENIZE(current_roster)) as id; 

-- Load players, turn their positions into a bag, and join them against
-- the team data generated above
players            = load 'baseball_player.tsv' as (name, id,
                        hall_of_fame_induction, current_team, position_s, 
                        bats, former_teams, batting_stats, baseball_almanac_id,
                        lifetime_batting_statistics);
player_position    = filter players by position_s is not null;
-- Replace spaces with underscores so that "relief pitcher" ends up
-- as one position and not two.
temp2              = foreach player_position generate name, current_team,
                        REPLACE(position_s, ' ', '_') as position,
                        batting_stats, lifetime_batting_statistics;
-- Replace the underscore separator with white space so I can use TOKENIZE to
-- turn the list into a bag.
temp1              = foreach temp2 generate name, current_team,
                        REPLACE(position, ',', ' ') as position, batting_stats,
                        lifetime_batting_statistics;
positioned_players = foreach temp1 generate name, current_team,
                        TOKENIZE(position) as position,
                        lifetime_batting_statistics;
joined             = join positioned_players by current_team,
                        team_players by id;
players_with_teams = foreach joined generate positioned_players::name as name,
                        team_players::name as team, position,
                        lifetime_batting_statistics;

-- Load lifetime batting stats, turn them into a map, and then join them
-- with the players for our final result
batting = load 'lifetime_batting_statistics.tsv' as (name,id, player,
            starting_season, ending_season, games, at_bats, hits, runs, doubles,
            triples, home_runs, grand_slams, rbis, bases_on_balls, ibbs,
            strikeouts, sacrifice_hits, sacrifice_flies, hit_by_pitch, gdp,
            batting_average, on_base_percentage, slugging_percentage, 
            last_statistics_season);
in_map  = foreach batting generate id, tomap(name, id, player, starting_season,
            ending_season, games, at_bats, hits, runs, doubles, triples,
            home_runs, grand_slams, rbis, bases_on_balls, ibbs, strikeouts
            sacrifice_hits, sacrifice_flies, hit_by_pitch, gdp, batting_average,
            on_base_percentage, slugging_percentage,
            last_statistics_season) as lifetime_batting;
almost  = join players_with_teams by lifetime_batting_statistics, in_map by id;
final   = foreach almost generate players_with_teams::name as name, team,
            position, lifetime_batting; 
store final into 'baseball';
