from ttb.data.trade_enum import Side


class TradeVote:
    def __init__(self, ticker: str, direction: Side, vote_source: str, vote_weight: float = None):
        self.ticker = ticker
        self.direction = direction
        self.vote_source = vote_source
        self.vote_weight = vote_weight

    def __str__(self):
        return f'ticker={self.ticker}, direction={self.direction}, vote_source={self.vote_source}, vote_weight={self.vote_weight}'
