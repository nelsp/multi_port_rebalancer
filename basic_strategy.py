class ConservativeStrategy(Strategy):
    """Conservative strategy: saves money, builds slowly"""

    def choose_auction_move(self, player, game_state):
        """Buy only cheap plants"""
        available_plants = game_state.current_market
        must_buy = (game_state.round_num == 1)

        if not available_plants or not StrategyUtils.can_buy_plant(player):
            return PlayerAction.auction_pass()

        affordable = StrategyUtils.get_affordable_plants(player, available_plants)
        if not affordable:
            return PlayerAction.auction_pass()

        if must_buy:
            # Choose cheapest affordable
            cheapest = min(affordable, key=lambda p: p.cost)
            discard = min(player.cards, key=lambda c: c.cost) if len(player.cards) >= 3 else None
            return PlayerAction.auction_open(cheapest, cheapest.cost, discard)

        # Only buy if we have enough money left (keep reserve)
        reserve = 20
        affordable_reserve = [p for p in affordable if p.cost <= player.money - reserve]
        if affordable_reserve:
            cheapest = min(affordable_reserve, key=lambda p: p.cost)
            discard = min(player.cards, key=lambda c: c.cost) if len(player.cards) >= 3 else None
            return PlayerAction.auction_open(cheapest, cheapest.cost, discard)

        return PlayerAction.auction_pass()

    def bid_in_auction(self, player, game_state, plant, current_bid, current_winner):
        """Conservative: Only bid on cheap plants, don't bid high"""
        min_bid = current_bid + 1
        max_bid = player.money

        # Only interested if current bid is still low
        reserve = 20  # Keep money in reserve
        if plant.cost <= 15 and min_bid <= plant.cost + 2 and min_bid <= max_bid - reserve:
            # Only bid minimum, don't escalate
            discard = min(player.cards, key=lambda c: c.cost) if len(player.cards) >= 3 else None
            return PlayerAction.auction_bid(min_bid, discard)
        return PlayerAction.auction_bid_pass()

    def choose_resources(self, player, game_state):
        """Buy minimal resources"""
        resources = game_state.resources
        purchases = {}
        capacities = StrategyUtils.get_resource_capacities(player)

        for card in player.cards:
            if card.resource == 'green':
                continue

            resource_type = card.resource
            if resource_type == 'nuclear':
                resource_type = 'uranium'
            elif resource_type == 'oil&gas':
                # Choose cheaper resource
                if 'oil' in resources and 'gas' in resources:
                    oil_cost = StrategyUtils.get_resource_cost(resources['oil'], 1)
                    gas_cost = StrategyUtils.get_resource_cost(resources['gas'], 1)
                    resource_type = 'oil' if oil_cost and gas_cost and oil_cost <= gas_cost else 'gas'
                elif 'oil' in resources:
                    resource_type = 'oil'
                elif 'gas' in resources:
                    resource_type = 'gas'
                else:
                    continue

            if resource_type in resources:
                # Only buy what's needed for one production
                current = player.resources.get(resource_type, 0)
                needed = card.resource_cost - current

                if needed > 0:
                    # Check availability and affordability
                    available_amount = resources[resource_type].count
                    amount_to_buy = min(needed, available_amount)

                    if amount_to_buy > 0:
                        cost = StrategyUtils.get_resource_cost(resources[resource_type], amount_to_buy)
                        if cost is not None and player.money >= cost:
                            purchases[resource_type] = purchases.get(resource_type, 0) + amount_to_buy

        return PlayerAction.resource_purchase(purchases)

    def choose_cities_to_build(self, player, game_state):
        """Build only if can afford easily"""
        available = StrategyUtils.get_available_cities(player, game_state)
        if not available:
            return PlayerAction.city_build([])

        # Check if game has ended - maximize powered cities
        if StrategyUtils.has_game_ended_with_players(game_state.players):
            current_powered = StrategyUtils.calculate_max_powered_cities(player)
            target_cities = min(len(available), current_powered - len(player.generators))
            if target_cities <= 0:
                target_cities = 1 if len(player.generators) == 0 else 0

            cities_to_build = []
            budget = player.money
            cities_with_cost = [(city, StrategyUtils.calculate_city_cost(player, city, game_state))
                               for city in available]
            cities_with_cost.sort(key=lambda x: x[1])

            for city, cost in cities_with_cost:
                if len(cities_to_build) >= target_cities or budget < cost:
                    break
                cities_to_build.append(city)
                budget -= cost

            return PlayerAction.city_build(cities_to_build)

        # Normal conservative behavior - save money if we have few resources
        if player.money < 30 and len(player.generators) > 0:
            return PlayerAction.city_build([])  # Save money

        # Build in cheapest city if we have money or need our first generator
        if player.money >= 30 or len(player.generators) == 0:
            city = min(available, key=lambda c: StrategyUtils.calculate_city_cost(player, c, game_state))
            return PlayerAction.city_build([city])

        return PlayerAction.city_build([])

    def choose_cities_to_power(self, player, game_state):
        """Conservative: Power enough to maintain cash flow, save resources if wealthy"""
        max_powered = StrategyUtils.calculate_max_powered_cities(player)

        # If we have enough money (>60E), consider saving resources for later
        if player.money > 60 and len(player.generators) >= 10:
            # Power fewer cities to conserve resources
            cities = max(0, max_powered - 2)
        else:
            # Otherwise power maximum
            cities = max_powered

        return PlayerAction.power_cities(cities)

