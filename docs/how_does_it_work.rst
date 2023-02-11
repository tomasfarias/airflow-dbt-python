.. _how_does_it_work:

How does it work?
=================

*airflow-dbt-python*'s main goal is to elevate *dbt* to a **first-class citizen** in Airflow. By this we mean that users of *dbt* can leverage as many Airflow features as possible, without breaking the assumptions that Airflow expects from any tasks. By this we also mean that Airflow should **enhance** your experience with *dbt* and not simply simulate the way you would run *dbt* in the command line.

To achieve this goal *airflow-dbt-python* provides Airflow operators, hooks, and other utilities. Moreover, hooks are also subdivided into:

* A ``DbtHook`` that abstracts all interaction with *dbt* internals.
* Subclasses of ``DbtRemoteHook`` that expose an interface to interact with *dbt* remote storages where project files are located (like AWS S3 or git repos).

.. graphviz:: how_does_it_work.dot

Philosophy
----------

**Airflow tasks should be independent**
    Operators and hooks don't make assumptions about the state of the filesystem, and instead setup everything they need for execution.

*dbt* operators
---------------

*airflow-dbt-python* provides one operator per *dbt* task. For example, ``DbtRunOperator`` can be used to execute a *dbt* run command, as if running ``dbt run ...`` in the CLI.

*dbt* operators are about task execution
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Operators **do not** concern themselves on how to interact with dbt internals. That is a task for *dbt* hooks.


*dbt* hooks
-----------

*airflow-dbt-python* provides a ``DbtHook`` to abstract all interactions with dbt. The main method users of ``DbtHook`` should be calling is ``DbtHook.run_dbt_task`` which takes any *dbt* command as it's first argument, and any configuration parameters keyword arguments. This method abstracts a lot setup required to run *dbt*, including enabling certain Airflow features that require some modification of how *dbt* works.

*dbt* hooks help preserve task independence
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In line with the Airflow's concept of a task:

> Tasks don’t pass information to each other by default, and run entirely independently

*dbt* hooks provide an isolated and ephemeral directory to execute dbt.


*dbt* remote hooks
------------------


*dbt* as a library
------------------

Our main challenge in achieving this goal is that *dbt* is committed to providing only a CLI interface (`at least for now <https://github.com/dbt-labs/dbt-core/issues/6356>`_).
