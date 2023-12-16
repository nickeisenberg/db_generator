from copy import deepcopy
import numpy as np
import datetime as dt

class SalSavStartGen:
    """
    This class attempts to produce a relatively believable salary, savings and 
    startdate for a person based on the average salary of their job.

    The startdate is randomly selected inbetween Jan 1, 2000 and Aug 15, 2023.
    
    The work duration is calculated as the amount of time one has spent 
    at the job between the startdate and Aug 15, 2023.
    
    If the avg_sal > 0 then the generated salary is based off of the sum 
    of two componets 
        1) max(normal(avg_sal, avg_sal / 4), avg_sal / 10)
        2) max(np.random.gamma(.1, avg_sal) - np.random.gamma(.1, avg_sal), 0)
    (1) is basically the average base salary and is bounded below by a tenth
    of the average salary. (2) basically is random noise that is prodominately
    zero, however can occasionally become very large, which then introduces an 
    outlier salary for those who happend to be at the very top of their
    field.

    If the avg_sal > 0 then the savings is based off of the 
    work duration and salary and is generated to be 
        np.random.normal(
            salary * work_duration / 4,
            salary / 8,
        )
    If this value is negative then that is taken to mean that the person is in 
    debt.
    If the avg_sal = 0, ie the person is unemployed, then this persons savings
    is simulated to be
        np.random.gamma(.5, 50) - np.random.gamma(.1, 50)
    There is no particular reason that I chose this for the savings of a 
    person with no salary other than the fact that I thought the histogram 
    after simulated 1000 samples looked nice.


    Parameter
    --------------------------------------------------
    avg_sal : int
        The average salary for the position

    Attributes
    --------------------------------------------------
    salary : float
        The generated salary given the average salary 
    startdate : datetime
        The generated startdate randomly selected between Jan 1, 2000 and 
        Aug 15, 2023.
    savings : float
        The generated savings given the average salary 

    View the Distributions
    --------------------------------------------------
    import matplotlib.pyplot as plt
    import numpy as np
    from faker import Faker
    import datetime as dt
    from copy import deepcopy
    
    _fk = Faker()

    # Non-zero salary
    avg_sal=100
    sav = np.array([])
    sal = np.array([])
    for i in range(10000):
        salsav = SalSavStartGen(avg_sal)
        sav = np.append(sav, salsav.savings)
        sal = np.append(sal, salsav.salary)
    
    sal.mean()
    sal.max()
    sav.mean()
    sav.max()
    
    fig, ax = plt.subplots(1, 2)
    ax[0].hist(sav, bins=100)
    ax[1].hist(sal, bins=100)
    plt.show()
    
    # Zero salary
    
    avg_sal=0
    sav = np.array([])
    for i in range(10000):
        salsav = SalSavStartGen(avg_sal)
        sav = np.append(sav, salsav.savings)
    
    sav.mean()
    sav.max()
    
    plt.hist(sav, bins=100)
    plt.show()
    """

    def __init__(self, avg, fkr):
        self.salary = self._salary_generator(avg)
        self.startdate, self.work_duration = self._startdate_generator(fkr)
        self.savings = self._savings_generator()

    @staticmethod
    def _salary_generator(avg_sal):
        if avg_sal == 0:
            return 0
        else:
            sal_noise_r = np.random.gamma(.1, avg_sal)
            sal_noise_r -= np.random.gamma(.1, avg_sal)
            sal_noise_l = np.abs(np.random.normal(avg_sal, avg_sal / 4))
            if sal_noise_r > 0:
                return sal_noise_r + sal_noise_l
            else:
                return max(sal_noise_l, avg_sal / 10)
    
     
    def _savings_generator(self):
        if self.salary == 0:
            return np.random.gamma(.5, 50) - np.random.gamma(.1, 50)
        else:
            saving = np.random.normal(
                self.salary * self.work_duration / 4,
                self.salary / 8,
            )
            return saving
    
    @staticmethod
    def _startdate_generator(
        fkr,
        start=dt.datetime(2000, 1, 1),
        end=dt.datetime(2023, 8, 15),
    ):
        start_date = deepcopy(dt.datetime.strftime(
            fkr.date_between(start, end), '%Y-%m-%d'
        ))

        work_duration = end - dt.datetime.strptime(start_date, '%Y-%m-%d')
        work_duration = work_duration.days / 365

        return start_date, work_duration
